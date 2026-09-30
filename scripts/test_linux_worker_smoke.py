#!/usr/bin/env python3
"""Opt-in isolated Linux worker smoke test. Copies four supplied fixtures.

Run on Linux after building the selected image. No production mounts are used.
Fixtures and evidence are retained. Test containers are stopped and retained with evidence.
"""
import argparse
import hashlib
import json
import os
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
    name = 'keeps-worker-smoke-' + run_id
    worker_name = name + '-worker'
    worker_created = False
    started = time.monotonic()
    container_user = str(os.getuid()) + ':' + str(os.getgid())
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

    def request(path, method='GET', auth=True, body=None):
        headers = {'Authorization': 'Bearer ' + token} if auth else {}
        data = None if body is None else json.dumps(body).encode()
        if body is not None:
            headers['Content-Type'] = 'application/json'
        req = urllib.request.Request(api + path, headers=headers, method=method, data=data)
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
        for kind, extensions in [('raw', ('arw', 'cr2', 'nef', 'dng')), ('jpeg', ('jpg', 'jpeg')), ('heif', ('heic', 'heif')), ('3fr', ('3fr',))]:
            candidates = [p for p in args.fixtures.rglob('*') if p.is_file() and not p.is_symlink() and p.suffix.lower().lstrip('.') in extensions and '.keeps-' not in p.name and '@eaDir' not in p.parts]
            if not candidates:
                raise RuntimeError('Missing fixture: ' + kind)
            source = min(candidates, key=lambda p: p.stat().st_size)
            target = originals / kind / source.name
            samples[kind] = dict(source=str(source), fixture=str(target), container='/originals/' + kind + '/' + source.name, hash=sha(source), originalHash=sha(source))
        report['samples'] = samples
        save()
        docker('run', '-d', '--user', container_user, '--name', name, '--cpuset-cpus', args.cpu, '--memory', '2g', '--memory-swap', '2g', '-p', '127.0.0.1:' + str(args.port) + ':2283', '-v', str(keeps) + ':/keeps', '-v', str(originals) + ':/originals', '-e', 'KEEPS_ROOT=/keeps', '-e', 'ORIGINAL_ROOT=/originals', '-e', 'KEEPS_LIBRARY_ID=smoke', '-e', 'KEEPS_LOCAL_CACHE_ENCODING_ENABLED=0', '-e', 'KEEPS_ACCESS_TOKEN=' + token, '-e', 'CONTROL_PLANE_PUBLIC_BASE_URL=' + api, '-e', 'CONTROL_PLANE_AUTO_CREATE_SCHEMA=1', '-e', 'KEEPS_SCAN_INTERVAL_SECONDS=3600', args.image)
        created = True
        wait(healthy)
        try:
            request('/libraries/smoke/cache-status', auth=False)
            raise AssertionError('Unauthenticated cache status accepted')
        except urllib.error.HTTPError as error:
            assert error.code == 401
        try:
            request('/libraries/smoke/worker/claim', method='POST', auth=False, body={'workerID': 'unauthorized'})
            raise AssertionError('Unauthenticated worker claim accepted')
        except urllib.error.HTTPError as error:
            assert error.code == 401
        check('health_and_auth')
        db_path = keeps / 'db/control_plane.sqlite'
        for sample in samples.values():
            target = Path(sample['fixture'])
            target.parent.mkdir()
            with Path(sample['source']).open('rb') as src, target.open('xb') as dst:
                shutil.copyfileobj(src, dst)
            assert sha(target) == sample['hash']
            shutil.copy2(Path(sample['source']), keeps / ('identity-before' + target.suffix))
        folders = request('/libraries/smoke/folders')['folders']
        request('/libraries/smoke/folders/' + folders[0]['id'] + '/scan', method='POST')
        wait(lambda: request('/libraries/smoke/cache-status')['totalAssets'] == 4)
        for sample in samples.values():
            sample['hash'] = sha(Path(sample['fixture']))
        work = base / 'worker'
        work.mkdir()
        docker('run', '-d', '--user', container_user, '--name', worker_name, '--network', 'host', '--cpus', '4', '--memory', '12g', '--entrypoint', 'python3', '-v', str(work) + ':/work', '-e', 'KEEPS_WORKER_URL=' + api, '-e', 'KEEPS_LIBRARY_ID=smoke', '-e', 'KEEPS_LOCAL_CACHE_ENCODING_ENABLED=0', '-e', 'KEEPS_API_TOKEN=' + token, '-e', 'KEEPS_WORKER_CONCURRENCY=4', args.image, '/usr/local/bin/linux_cache_worker.py')
        worker_created = True

        def ready():
            status = request('/libraries/smoke/cache-status')
            report['lastCacheStatus'] = status
            save()
            assert status['batchLimit'] == 20 and status['concurrency'] == 1 and status['restSeconds'] == 60
            assert status['counts']['processing'] <= 4
            assert status['localEncodingEnabled'] is False
            if status['counts']['failed']:
                raise AssertionError('Cache failure: ' + json.dumps(status))
            return status['counts']['ready'] == 4 and status['counts']['pending'] == 0 and status['counts']['processing'] == 0
        wait(ready)
        db_path = keeps / 'db/control_plane.sqlite'
        with readonly(db_path) as db:
            assert db.execute('PRAGMA user_version').fetchone()[0] == 9
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
                    assert standard.name == Path(sample['container']).stem + ".heic"
                    tags = json.loads(docker("exec", name, "exiftool", "-j", "-Software", "-XMP-xmp:CreatorTool", "-XMP-xmpMM:OriginalDocumentID", str(standard)))[0]
                    assert tags["Software"] == "Keeps" and tags["CreatorTool"] == "Keeps"
                    root = db.execute("SELECT root_id FROM catalog_identity_roots WHERE asset_id=?", (asset,)).fetchone()[0]
                    assert tags["OriginalDocumentID"] == "xmp.did:" + root
                    row = db.execute('SELECT asset_id,content_hash FROM catalog_version_paths WHERE path=?', (str(standard),)).fetchone()
                    assert row == (asset, desc['version'])
                    evidence = json.loads(db.execute('SELECT evidence FROM catalog_versions WHERE asset_id=? AND content_hash=?', row).fetchone()[0])
                    assert evidence['generatedFrom'] == sample['hash']
                    physical = originals / standard.relative_to('/originals')
                    assert sha(physical) == desc['version']
            check('schema9_identity_no_ledger_and_raw_same_asset_standard')
        with readonly(db_path) as db:
            tasks = db.execute('SELECT id FROM remote_cache_tasks WHERE completed=1').fetchall()
            assert len(tasks) == 4
        for (task_id,) in tasks:
            assert request('/libraries/smoke/worker/tasks/' + task_id + '/complete', method='POST', body={})['ok']
        check('all_four_remote_tasks_and_duplicate_complete')
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
            assert sha(Path(sample['source'])) == sample['originalHash']
        state = json.loads(docker('inspect', name))[0]
        assert not state['State']['OOMKilled'] and state['HostConfig']['CpusetCpus'] == args.cpu
        assert state['HostConfig']['Memory'] == 2 * 1024**3
        assert len(state['Mounts']) == 2 and all(mount['Source'].startswith(str(base) + '/') for mount in state['Mounts'])
        worker_state = json.loads(docker('inspect', worker_name))[0]
        assert worker_state['State']['Running'] and not worker_state['State']['OOMKilled']
        assert len(worker_state['Mounts']) == 1 and worker_state['Mounts'][0]['Source'] == str(work)
        report['containers'] = {'api': name, 'worker': worker_name}
        check('rescan_stable_original_hashes_and_resource_limits')
        with sqlite3.connect(keeps / 'db' / 'control_plane.sqlite') as db:
            db.execute("UPDATE catalog_files SET holder='legacy-mac' WHERE asset_id=? AND role='raw_original'", (asset_ids['raw'],))
        for kind, sample in samples.items():
            metadata_path = sample['container'] + ('.xmp' if kind == '3fr' else '')
            root = docker('exec', name, 'exiftool', '-s3', '-XMP-xmpMM:OriginalDocumentID', metadata_path)
            with readonly(db_path) as db:
                expected_root = db.execute('SELECT root_id FROM catalog_identity_roots WHERE asset_id=?',(asset_ids[kind],)).fetchone()[0]
            assert root == 'xmp.did:' + expected_root
        check('identity_root_metadata_and_3fr_sidecar')
        def decoded_hash(path, kind, label):
            if kind == 'raw':
                output = '/keeps/' + label + '.ppm'
                docker('exec', name, 'dcraw_emu', '-w', '-o', '1', '-Z', output, path)
                return sha(keeps / (label + '.ppm'))
            if kind == 'heif':
                output = '/keeps/' + label + '.tiff'
                docker('exec', name, 'heif-convert', '--disable-limits', '--quiet', path, output)
                path = output
            process = subprocess.Popen([args.docker,'exec',name,'convert',path,'rgb:-'],stdout=subprocess.PIPE)
            digest=hashlib.sha256()
            for block in iter(lambda:process.stdout.read(1024*1024),b''):
                digest.update(block)
            assert process.wait()==0
            return digest.hexdigest()
        for kind in ('raw','jpeg','heif'):
            sample=samples[kind]
            baseline='/keeps/identity-before' + Path(sample['fixture']).suffix
            assert decoded_hash(baseline,kind,kind+'-before') == decoded_hash(sample['container'],kind,kind+'-after')
        assert sha(Path(samples['3fr']['fixture'])) == samples['3fr']['originalHash']
        check('metadata_write_preserves_raw_jpeg_heif_pixels_and_3fr_bytes')

        old_raw = Path(samples['raw']['fixture'])
        moved_directory = originals / 'raw-moved'
        old_raw.parent.rename(moved_directory)
        moved_raw = moved_directory / old_raw.name
        moved_container = '/originals/raw-moved/' + old_raw.name
        def moved_indexed():
            with readonly(keeps / 'db' / 'control_plane.sqlite') as db:
                row = db.execute('SELECT asset_id FROM catalog_paths WHERE path=?', (moved_container,)).fetchone()
                stale = db.execute('SELECT count(*) FROM catalog_paths WHERE path=?', (samples['raw']['container'],)).fetchone()[0]
                return row is not None and row[0] == asset_ids['raw'] and stale == 0
        wait(moved_indexed)
        assert sha(moved_raw) == samples['raw']['hash']
        with readonly(keeps / 'db' / 'control_plane.sqlite') as db:
            assert db.execute('SELECT count(*) FROM catalog_assets').fetchone()[0] == 4
        check('directory_move_preserves_asset_and_retires_old_path')
        report['elapsedSeconds'] = round(time.monotonic() - started, 2)
        report['status'] = 'PASS'
        save()
    except Exception:
        report['status'] = 'FAIL'
        report['error'] = traceback.format_exc()
        save()
        raise
    finally:
        for container, exists in [(worker_name, worker_created), (name, created)]:
            if exists:
                subprocess.run([args.docker, 'stop', '-t', '30', container], check=True)
                logs = subprocess.run([args.docker, 'logs', container], text=True, capture_output=True)
                (base / (container + '.log')).write_text(logs.stdout + logs.stderr)
        print('Evidence: ' + str(base / 'report.json'), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixtures', type=Path, required=True)
    parser.add_argument('--root', type=Path, default=Path('/tmp/keeps-linux-worker-smoke'))
    parser.add_argument('--image', required=True)
    parser.add_argument('--port', type=int, default=2290)
    parser.add_argument('--cpu', default='0')
    parser.add_argument('--deadline', type=int, default=1200)
    parser.add_argument('--docker', default='docker')
    args = parser.parse_args()
    args.root = args.root.resolve()
    args.fixtures = args.fixtures.resolve()
    run_test(args)


if __name__ == '__main__':
    main()
