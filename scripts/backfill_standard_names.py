#!/usr/bin/env python3
"""Offline, one-time migration of database-proven Keeps generated HEIC names.

Default is a read-only plan. Apply requires --apply --service-stopped and a fresh
--backup directory. Keep services stopped after any failure: manifest.json plus
both SQLite backups and original generated-file copies support recovery.
"""
import argparse
from collections import ChainMap
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def save(path, data):
    temporary = path.with_suffix('.tmp')
    with open(temporary, 'w') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)


def progress(stage, count, total, manifest=None, path=None):
    if count % 50 and count != total:
        return
    status = {'stage': stage, 'processed': count, 'total': total}
    if manifest is not None:
        manifest['progress'] = status
        save(path, manifest)
    print(json.dumps(status), flush=True)


def publish(stage, target):
    try:
        os.link(stage, target)
    except OSError as error:
        if error.errno != errno.EXDEV:
            raise
        with open(stage, 'rb') as source, open(target, 'xb') as output:
            shutil.copyfileobj(source, output)
            output.flush()
            os.fsync(output.fileno())


def plan(db, root):
    root = root.resolve()
    reserved = {r[0] for r in db.execute('SELECT path FROM catalog_paths UNION SELECT path FROM catalog_version_paths')}
    rows = db.execute("SELECT p.library_id,p.asset_id,p.path,p.content_hash,json_extract(v.evidence,'$.generatedFrom') FROM catalog_paths p JOIN catalog_versions v USING(library_id,asset_id,content_hash) WHERE json_extract(v.evidence,'$.generatedFrom') IS NOT NULL ORDER BY p.path").fetchall()
    result = []
    for lib, asset, old, old_hash, source_hash in rows:
        source = Path(old)
        match = re.fullmatch(r'(.+)\.keeps-([0-9a-f]{64})\.heic', source.name)
        if not match:
            continue
        if match[2] != source_hash:
            raise RuntimeError(f'Filename/source evidence mismatch: {old}')
        if source.is_symlink() or not source.is_file() or root not in source.resolve().parents:
            raise RuntimeError(f'Missing, symlink or out-of-root generated file: {old}')
        if digest(source) != old_hash:
            raise RuntimeError(f'Generated file changed since indexing: {old}')
        stem = Path(match[1]).stem
        index = 0
        while True:
            target = source.with_name(stem + (f'.{index}' if index else '') + '.heic')
            if not os.path.lexists(target) and str(target) not in reserved:
                break
            index += 1
        reserved.add(str(target))
        result.append(dict(library=lib, asset=asset, old=old, new=str(target), oldHash=old_hash, sourceHash=source_hash))
        progress('plan-hash-verification', len(result), len(rows))
    return result


def rewrite(value, replacements, details):
    if isinstance(value, str):
        return replacements.get(value, value)
    if isinstance(value, list):
        return [rewrite(v, replacements, details) for v in value]
    if isinstance(value, dict):
        detail = details.get(value.get('path')) or details.get(value.get('contentHash'))
        result = {k: rewrite(v, replacements, details) for k, v in value.items()}
        if detail:
            for key in ('originalFilename', 'filename'):
                if result.get(key) == Path(detail['old']).name:
                    result[key] = Path(detail['new']).name
            if 'sizeBytes' in result:
                result['sizeBytes'] = detail['size']
            if 'mtimeNs' in result:
                result['mtimeNs'] = str(detail['mtimeNs']) if isinstance(value['mtimeNs'], str) else detail['mtimeNs']
        return result
    return value


def update_database(db, schema, items):
    replacements, details, asset_details = {}, {}, {}
    for item in items:
        for key, value in [(item['old'], item['new']), (item['oldHash'], item['newHash'])]:
            if key in replacements and replacements[key] != value:
                raise RuntimeError('Ambiguous generated content mapping')
            replacements[key] = value
            details[key] = item
        asset_details.setdefault((item.get('library'), item.get('asset')), {})[item['oldHash']] = item
    tables = ['catalog_assets', 'catalog_files', 'catalog_paths', 'catalog_versions', 'catalog_version_paths', 'catalog_defaults', 'derivative_objects', 'media_cache', 'remote_cache_tasks', 'photos', 'videos'] if schema == 'main' else ['files', 'jobs']
    for table in tables:
        columns = [r[1] for r in db.execute(f'PRAGMA {schema}.table_info("{table}")')]
        for row in db.execute(f'SELECT rowid,* FROM {schema}."{table}"').fetchall():
            values = dict(zip(columns, row[1:]))
            changes = {}
            row_details = ChainMap(asset_details.get((values.get('library_id'), values.get('asset_id', values.get('id'))), {}), details)
            detail = row_details.get(values.get('path')) or row_details.get(values.get('content_hash'))
            for column, value in values.items():
                if not isinstance(value, str):
                    continue
                new = replacements.get(value, value)
                if value.startswith(('{', '[')):
                    parsed = json.loads(value)
                    changed = rewrite(parsed, replacements, row_details)
                    if changed != parsed:
                        new = json.dumps(changed, ensure_ascii=False, separators=(',', ':'))
                if new != value:
                    changes[column] = new
            if detail and values.get('filename') == Path(detail['old']).name:
                changes['filename'] = Path(detail['new']).name
            if detail and 'size' in columns:
                changes['size'] = detail['size']
            if detail and 'mtime_ns' in columns:
                changes['mtime_ns'] = detail['mtimeNs']
                if 'metadata_stamp' in columns:
                    changes['metadata_stamp'] = ''
            if changes:
                sets = ','.join(f'"{c}"=?' for c in changes)
                db.execute(f'UPDATE {schema}."{table}" SET {sets} WHERE rowid=?', [*changes.values(), row[0]])


def apply(args, items):
    backup = Path(args.backup)
    backup.mkdir(parents=True, exist_ok=False)
    (backup / 'originals').mkdir()
    (backup / 'staged').mkdir()
    manifest = {'state': 'preparing', 'controlDB': args.control_db, 'jobsDB': args.jobs_db, 'items': items}
    save(backup / 'manifest.json', manifest)
    for name, path in [('control', args.control_db), ('jobs', args.jobs_db)]:
        with sqlite3.connect(f'file:{path}?mode=ro', uri=True) as source, sqlite3.connect(backup / f'{name}.sqlite') as destination:
            source.backup(destination)
    progress('copying', 0, len(items), manifest, backup / 'manifest.json')
    for i, item in enumerate(items):
        if digest(item['old']) != item['oldHash']:
            raise RuntimeError('Source changed after plan')
        item['backup'] = str(backup / 'originals' / f'{i}.heic')
        item['stage'] = str(backup / 'staged' / f'{i}.heic')
        shutil.copy2(item['old'], item['backup'])
        shutil.copy2(item['old'], item['stage'])
        progress('copying', i + 1, len(items), manifest, backup / 'manifest.json')
    save(backup / 'manifest.json', manifest)
    progress('metadata', 0, len(items), manifest, backup / 'manifest.json')
    if items:
        subprocess.run([args.exiftool, '-overwrite_original', '-XMP-xmp:CreatorTool=Keeps', '-Software=Keeps', str(backup / 'staged')], check=True)
    if items:
        tags = json.loads(subprocess.check_output([args.exiftool, '-json', '-CreatorTool', '-Software', str(backup / 'staged')], text=True))
        if len(tags) != len(items) or any(t.get('CreatorTool') != 'Keeps' or t.get('Software') != 'Keeps' for t in tags):
            raise RuntimeError('Keeps metadata readback failed')
    progress('staged-hash-verification', 0, len(items), manifest, backup / 'manifest.json')
    for i, item in enumerate(items):
        item['newHash'] = digest(item['stage'])
        item['size'] = os.stat(item['stage']).st_size
        progress('staged-hash-verification', i + 1, len(items), manifest, backup / 'manifest.json')
    hashes = {}
    for item in items:
        prior = hashes.setdefault(item['oldHash'], item['newHash'])
        if prior != item['newHash']:
            raise RuntimeError('Identical generated content received different metadata outputs')
    manifest['state'] = 'staged'
    save(backup / 'manifest.json', manifest)
    progress('publishing', 0, len(items), manifest, backup / 'manifest.json')
    for i, item in enumerate(items):
        publish(item['stage'], item['new'])
        item['mtimeNs'] = os.stat(item['new']).st_mtime_ns
        progress('publishing', i + 1, len(items), manifest, backup / 'manifest.json')
    manifest['state'] = 'published-before-database'
    save(backup / 'manifest.json', manifest)
    with sqlite3.connect(args.control_db) as db:
        db.execute('ATTACH DATABASE ? AS jobsdb', [args.jobs_db])
        db.execute('BEGIN IMMEDIATE')
        update_database(db, 'main', items)
        update_database(db, 'jobsdb', items)
        db.commit()
    manifest['state'] = 'database-committed'
    save(backup / 'manifest.json', manifest)
    progress('final-verification', 0, len(items), manifest, backup / 'manifest.json')
    for i, item in enumerate(items):
        if digest(item['backup']) != item['oldHash'] or digest(item['old']) != item['oldHash'] or digest(item['new']) != item['newHash']:
            raise RuntimeError('Post-publication verification failed; leave service stopped')
        os.unlink(item['old'])  # Only database-proven generated standard, with verified backup.
        progress('final-verification', i + 1, len(items), manifest, backup / 'manifest.json')
    manifest['state'] = 'complete'
    save(backup / 'manifest.json', manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control-db', required=True)
    parser.add_argument('--jobs-db', required=True)
    parser.add_argument('--photo-root', required=True)
    parser.add_argument('--plan', required=True)
    parser.add_argument('--backup')
    parser.add_argument('--exiftool', default='exiftool')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--service-stopped', action='store_true')
    args = parser.parse_args()
    if args.apply and (not args.service_stopped or not args.backup):
        parser.error('--apply requires --service-stopped and --backup')
    with sqlite3.connect(f'file:{args.control_db}?mode=ro', uri=True) as db:
        items = plan(db, Path(args.photo_root).resolve())
    save(Path(args.plan), {'count': len(items), 'items': items})
    print(json.dumps({'planned': len(items), 'plan': args.plan}))
    if args.apply:
        apply(args, items)
        print(json.dumps({'migrated': len(items), 'backup': args.backup}))


if __name__ == '__main__':
    main()
