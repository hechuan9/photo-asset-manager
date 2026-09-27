"""Read-only checks against a running NAS deployment; never prints credentials or URLs."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import statistics
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--root', required=True)
    parser.add_argument('--backup', required=True)
    parser.add_argument('--base-url', default='http://127.0.0.1:2283')
    parser.add_argument('--library', default='local-library')
    parser.add_argument('--container', default='keeps-control-plane')
    parser.add_argument('--report', required=True)
    args = parser.parse_args()
    settings = dict(line.split('=', 1) for line in Path(args.env_file).read_text().splitlines()
                    if '=' in line and not line.startswith('#'))
    token = settings['KEEPS_ACCESS_TOKEN']
    results = []
    def passed(name, **detail):
        results.append(dict(test=name, status='PASS', **detail))
        print('PASS: ' + name, flush=True)
    def request(path, auth=True):
        headers = {'Authorization': 'Bearer ' + token} if auth else {}
        start = time.monotonic()
        with urllib.request.urlopen(urllib.request.Request(args.base_url + path, headers=headers), timeout=30) as response:
            data = response.read()
        return data, (time.monotonic() - start) * 1000
    def get(path, auth=True):
        return json.loads(request(path, auth)[0])
    def status(path, expected, auth=True):
        try:
            request(path, auth)
            raise AssertionError('expected HTTP ' + str(expected))
        except urllib.error.HTTPError as error:
            assert error.code == expected, (error.code, expected)
    prefix = '/libraries/' + urllib.parse.quote(args.library, safe='')
    report = {'checkedAtUTC': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'checks': results}
    try:
        inspect = json.loads(subprocess.check_output(['docker','inspect',args.container]))[0]
        state = inspect['State']
        assert state['Running'] and state['Health']['Status'] == 'healthy'
        assert not state['OOMKilled']
        mounts = [m for m in inspect['Mounts'] if m['Destination'].startswith('/originals/')]
        assert len(mounts) == 4 and all(not m['RW'] for m in mounts)
        passed('container_health_and_original_mounts', image=inspect['Image'], restarts=inspect['RestartCount'], readOnlyMounts=len(mounts))
        assert get('/healthz', False) == {'status':'ok'}
        status(prefix+'/assets',401,False)
        status(prefix+'/ops',404)
        status(prefix+'/assets?cursor=invalid',422)
        status(prefix+'/assets?minRating=9',422)
        passed('health_authentication_and_invalid_queries')
        initial_jobs = get(prefix+'/jobs')['jobs']
        page = get(prefix+'/assets?limit=50&sort=capture_asc')
        assert page['items'] and len({a['id'] for a in page['items']}) == len(page['items'])
        next_page = get(prefix+'/assets?limit=50&sort=capture_asc&cursor='+page['nextCursor'])
        assert not {a['id'] for a in page['items']} & {a['id'] for a in next_page['items']}
        item = page['items'][0]
        assert get(prefix+'/assets/'+item['id'].upper())['id'] == item['id']
        rated = get(prefix+'/assets?minRating=4&limit=30')
        assert all(a['rating'] >= 4 for a in rated['items'])
        trashed = get(prefix+'/assets?trashed=true&limit=30')
        assert all(a['trashed'] for a in trashed['items'])
        counts = get(prefix+'/counts')
        assert counts['all'] >= page['total']
        directories = get(prefix+'/directories')['directories']
        assert all(d['path'].startswith('/originals/') and d['count'] > 0 for d in directories)
        passed('catalog_pagination_filters_detail_directories', assets=page['total'], directoryCount=len(directories), ratedSample=len(rated['items']))
        previews = [a['preview'] for a in page['items'] if a.get('preview')][:5]
        assert previews
        sizes = []
        for preview in previews:
            path = urllib.parse.urlsplit(preview['downloadURL']).path
            data, _ = request(path,False)
            assert hashlib.sha256(data).hexdigest() == preview['version']
            sizes.append(len(data))
        passed('signed_preview_download_sha256', samples=len(sizes), bytes=sizes)
        endpoints = ['/healthz',prefix+'/counts',prefix+'/assets?limit=100',prefix+'/jobs']*8
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            timings = list(pool.map(lambda p:request(p)[1],endpoints))
        passed('concurrent_reads_during_scan', requests=len(timings), concurrency=4, medianMs=round(statistics.median(timings),2), p95Ms=round(sorted(timings)[int(len(timings)*.95)-1],2), maxMs=round(max(timings),2))
        root = Path(args.root)
        db = sqlite3.connect('file:'+str(root/'db/control_plane.sqlite')+'?mode=ro',uri=True,timeout=30)
        assert db.execute('PRAGMA quick_check').fetchall() == [('ok',)]
        assert db.execute('PRAGMA user_version').fetchone()[0] == 1
        db.execute('ATTACH DATABASE ? AS baseline',('file:'+args.backup+'?mode=ro',))
        old_count = db.execute('SELECT count(*) FROM baseline.ledger_events').fetchone()[0]
        changed = db.execute('''SELECT count(*) FROM baseline.ledger_events b LEFT JOIN main.ledger_events c ON c.op_id=b.op_id
            WHERE c.op_id IS NULL OR c.payload_json<>b.payload_json OR c.payload_hash<>b.payload_hash
            OR c.library_id<>b.library_id OR c.global_seq<>b.global_seq''').fetchone()[0]
        assert changed == 0
        missing = db.execute("""SELECT count(*) FROM (SELECT DISTINCT library_id,lower(json_extract(payload_json,'$.assetSnapshotDeclared.snapshot.assetID')) AS id
            FROM baseline.ledger_events WHERE op_type='asset_snapshot_declared') b
            LEFT JOIN main.catalog_assets a ON a.library_id=b.library_id AND a.id=b.id WHERE a.id IS NULL""").fetchone()[0]
        assert missing == 0
        db.close()
        jobs = sqlite3.connect('file:'+str(root/'db/jobs.sqlite')+'?mode=ro',uri=True,timeout=30)
        assert jobs.execute('PRAGMA quick_check').fetchall() == [('ok',)]
        file_errors = jobs.execute('SELECT count(*) FROM files WHERE error IS NOT NULL').fetchone()[0]
        jobs.close()
        passed('databases_integrity_and_complete_legacy_preservation', legacyEvents=old_count, changedLegacyEvents=changed, missingLegacyAssets=missing)
        final_jobs = get(prefix+'/jobs')['jobs']
        before = {j['id']:j for j in initial_jobs}
        progress = []
        for j in final_jobs:
            if j['id'] in before:
                previous = before[j['id']]
                delta = sum(j[k]-previous[k] for k in ('processed','skipped','failed'))
                progress.append(dict(status=j['status'],processed=j['processed'],skipped=j['skipped'],failed=j['failed'],progressDelta=delta))
        assert progress and any(p['progressDelta']>0 or p['status']=='completed' for p in progress), 'no scan progress during checks'
        assert file_errors == 0, 'production file processing errors require diagnosis'
        passed('background_scan_continues', jobs=progress, fileErrors=file_errors)
        report['status'] = 'PASS'
    except Exception as error:
        report['status'] = 'FAIL'
        report['errorType'] = type(error).__name__
        raise
    finally:
        Path(args.report).write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')


if __name__ == '__main__':
    main()
