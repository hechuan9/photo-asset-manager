#!/usr/bin/env python3
"""Opt-in real-media NAS smoke test. Copies four originals; never mounts production photos.

Run explicitly as a script on the NAS after building the selected image.
Fixtures and evidence are retained. Only this run's test container is removed.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import time
import traceback
import urllib.request
import urllib.error
import uuid


def sha(path):
    value = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def readonly(path):
    return sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)


def run_test(args):
    run_id = time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6]
    base = args.root / run_id
    base.mkdir(parents=True)
    originals, keeps = base / 'originals', base / 'keeps'
    originals.mkdir()
    keeps.mkdir()
    name = 'keeps-central-smoke-' + run_id
    token = 'isolated-smoke-' + uuid.uuid4().hex
    api = 'http://127.0.0.1:' + str(args.port)
    deadline = time.monotonic() + args.deadline
    report = {'status': 'RUNNING', 'checks': [], 'fixtureRoot': str(base)}
    created = False

    def save():
        (base / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))

    def check(label, **evidence):
        report['checks'].append(dict(test=label, status='PASS', **evidence))
        save()
        print('PASS ' + label, flush=True)

    def docker(*argv):
        return subprocess.check_output([args.docker, *argv], text=True).strip()

    def request(path, method='GET', auth=True):
        headers = {'Authorization': 'Bearer ' + token} if auth else {}
        req = urllib.request.Request(api + path, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.load(response)

    def wait(predicate):
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(2)
        raise TimeoutError('Smoke test exceeded overall deadline')

    def healthy():
        try:
            return request('/healthz', auth=False)
        except (OSError, urllib.error.URLError):
            return False

    try:
        samples = {}
        with readonly(args.source_db) as db:
            for kind, extensions in [('raw', ('arw',)), ('jpeg', ('jpg', 'jpeg')), ('heif', ('heic', 'heif')), ('3fr', ('3fr',))]:
                terms = ' OR '.join('lower(path) LIKE ?' for _ in extensions)
                candidates = [Path(row[0]) for row in db.execute('SELECT path FROM catalog_paths WHERE path LIKE ? AND (' + terms + ') LIMIT 100', ('/volume2/photo/%', *('%.' + ext for ext in extensions)))]
                candidates = [path for path in candidates if path.is_file() and not path.is_symlink()]
                if not candidates:
                    raise RuntimeError('Missing real sample: ' + kind)
                source = min(candidates, key=lambda path: path.stat().st_size)
                folder = originals / kind
                folder.mkdir()
                target = folder / source.name
                with source.open('rb') as src, target.open('xb') as dst:
                    shutil.copyfileobj(src, dst)
                expected = sha(source)
                assert sha(target) == expected
                samples[kind] = dict(source=str(source), fixture=str(target), container='/originals/' + kind + '/' + source.name, hash=expected)
        report['samples'] = samples
        save()
        docker('run', '-d', '--name', name, '--cpuset-cpus', args.cpu, '--memory', '2g', '--memory-swap', '2g', '-p', '127.0.0.1:' + str(args.port) + ':2283', '-v', str(keeps) + ':/keeps', '-v', str(originals) + ':/originals', '-e', 'KEEPS_ROOT=/keeps', '-e', 'ORIGINAL_ROOT=/originals', '-e', 'KEEPS_LIBRARY_ID=smoke', '-e', 'KEEPS_ACCESS_TOKEN=' + token, '-e', 'CONTROL_PLANE_PUBLIC_BASE_URL=' + api, '-e', 'CONTROL_PLANE_AUTO_CREATE_SCHEMA=1', '-e', 'KEEPS_SCAN_INTERVAL_SECONDS=3600', args.image)
        created = True
        wait(healthy)
        try:
            request('/libraries/smoke/cache-status', auth=False)
            raise AssertionError('Unauthenticated cache status accepted')
        except urllib.error.HTTPError as error:
            assert error.code == 401
        check('health_and_auth')

        def ready():
            status = request('/libraries/smoke/cache-status')
            report['lastCacheStatus'] = status
            save()
            assert status['batchLimit'] == 20 and status['concurrency'] == 1 and status['restSeconds'] == 60
            assert status['counts']['processing'] <= 1
            if status['counts']['failed']:
                raise AssertionError('Cache failure: ' + json.dumps(status))
            return status['counts']['ready'] == 4 and status['counts']['pending'] == 0 and status['counts']['processing'] == 0
        wait(ready)
        db_path = keeps / 'db/control_plane.sqlite'
        with readonly(db_path) as db:
            assert db.execute('PRAGMA user_version').fetchone()[0] == 7
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            assert not {'ledger_events', 'device_states', 'sync_conflicts', 'archive_receipts'} & tables
            assert db.execute('SELECT count(*) FROM catalog_assets').fetchone()[0] == 4
            asset_ids = {}
            for kind, sample in samples.items():
                asset = db.execute('SELECT asset_id FROM catalog_paths WHERE path=?', (sample['container'],)).fetchone()[0]
                asset_ids[kind] = asset
                desc = json.loads(db.execute('SELECT standard FROM media_cache WHERE asset_id=?', (asset,)).fetchone()[0])
                if kind in ('jpeg', 'heif'):
                    assert desc['path'] == sample['container']
                elif kind == 'raw':
                    standard = Path(desc['path'])
                    assert standard.parent == Path(sample['container']).parent
                    row = db.execute('SELECT asset_id,content_hash FROM catalog_version_paths WHERE path=?', (str(standard),)).fetchone()
                    assert row == (asset, desc['version'])
                    evidence = json.loads(db.execute('SELECT evidence FROM catalog_versions WHERE asset_id=? AND content_hash=?', row).fetchone()[0])
                    assert evidence['generatedFrom'] == sample['hash']
                    physical = originals / standard.relative_to('/originals')
                    assert sha(physical) == desc['version']
            check('schema7_no_ledger_and_raw_same_asset_standard')
        page = request('/libraries/smoke/assets?limit=100')
        assert len(page['items']) == 4
        for asset in page['items']:
            thumb = asset['thumbnail']
            assert max(thumb['width'], thumb['height']) <= 512
            with urllib.request.urlopen(thumb['downloadURL'], timeout=30) as response:
                assert hashlib.sha256(response.read()).hexdigest() == thumb['version'].split(':')[-1]
            if asset['id'] == asset_ids['3fr']:
                assert not asset.get('standard')
            else:
                standard = asset['standard']
                with urllib.request.urlopen(standard['downloadURL'], timeout=30) as response:
                    assert hashlib.sha256(response.read()).hexdigest() == standard['version']
        check('signed_download_hashes_jpeg_heif_reuse_3fr_thumbnail_only')
        folders = request('/libraries/smoke/folders')['folders']
        job = request('/libraries/smoke/folders/' + folders[0]['id'] + '/scan', method='POST')
        def rescanned():
            with readonly(keeps / 'db/jobs.sqlite') as db:
                row = db.execute('SELECT status,error FROM jobs WHERE id=?', (job['id'],)).fetchone()
                if row and row[0] == 'failed':
                    raise AssertionError('Rescan failed: ' + str(row[1]))
                return row and row[0] == 'completed'
        wait(rescanned)
        with readonly(db_path) as db:
            assert db.execute('SELECT count(*) FROM catalog_assets').fetchone()[0] == 4
            for kind, sample in samples.items():
                assert db.execute('SELECT asset_id FROM catalog_paths WHERE path=?', (sample['container'],)).fetchone()[0] == asset_ids[kind]
        for sample in samples.values():
            assert sha(Path(sample['fixture'])) == sample['hash']
            assert sha(Path(sample['source'])) == sample['hash']
        state = json.loads(docker('inspect', name))[0]
        assert not state['State']['OOMKilled'] and state['HostConfig']['CpusetCpus'] == args.cpu
        assert state['HostConfig']['Memory'] == 2 * 1024**3
        assert len(state['Mounts']) == 2 and all(mount['Source'].startswith(str(base) + '/') for mount in state['Mounts'])
        check('rescan_stable_original_hashes_and_resource_limits')
        report['status'] = 'PASS'
        save()
    except Exception:
        report['status'] = 'FAIL'
        report['error'] = traceback.format_exc()
        save()
        raise
    finally:
        if created:
            logs = subprocess.run([args.docker, 'logs', name], text=True, capture_output=True)
            (base / 'container.log').write_text(logs.stdout + logs.stderr)
            subprocess.run([args.docker, 'rm', '-f', name], check=True)
        print('Evidence: ' + str(base / 'report.json'), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-db', type=Path, default=Path('/volume2/myphoto/keeps/db/control_plane.sqlite'))
    parser.add_argument('--root', type=Path, default=Path('/volume2/docker/keeps/releases/central-db-20260929/smoke'))
    parser.add_argument('--image', default='keeps-server:central-db-20260929')
    parser.add_argument('--port', type=int, default=2290)
    parser.add_argument('--cpu', default='3')
    parser.add_argument('--deadline', type=int, default=1200)
    parser.add_argument('--docker', default='/usr/local/bin/docker')
    run_test(parser.parse_args())


if __name__ == '__main__':
    main()
