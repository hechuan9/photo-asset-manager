#!/usr/bin/env python3
"""Offline DB-only migration. Caller must stop Keeps before --apply.

Manifest is a JSON array: {library_id, path, asset_id, content_hash, role, size}. Paths are NAS
absolute paths. Files are stat-checked, never modified or content-hash verified.
"""
from contextlib import closing
import argparse
import collections
import datetime
import json
import tempfile
from pathlib import Path
import sqlite3
import stat

MAPPINGS = (
    ('/originals/library', '/volume2/photo'),
    ('/originals/raw-unprocessed', '/volume2/myphoto/未处理Raw'),
    ('/originals/raw-processed', '/volume2/myphoto/已处理Raw'),
    ('/originals/personal', '/volume2/myphoto/和川专属'),
)


def mapped(path):
    if path == '/originals':
        return '/volume2'
    for old, new in MAPPINGS:
        if path == old or path.startswith(old + '/'):
            return new + path[len(old):]
    if path.startswith('/originals/'):
        raise ValueError('Unmapped originals path: ' + path)
    return path


def integrity(db, schema='main'):
    result = db.execute(f'PRAGMA {schema}.integrity_check').fetchall()
    if result != [('ok',)]:
        raise ValueError(f'{schema} integrity_check failed: {result}')


def checked_file(path, size):
    candidate = Path(path)
    if not candidate.is_absolute() or '..' in candidate.parts:
        raise ValueError('Manifest path must be absolute without traversal')
    if not any(path == root or path.startswith(root + '/') for _, root in MAPPINGS):
        raise ValueError('Manifest path is outside configured NAS roots: ' + path)
    for component in [*reversed(candidate.parents), candidate]:
        try:
            info = component.lstat()
        except FileNotFoundError:
            return 'missing'
        if stat.S_ISLNK(info.st_mode):
            return 'symlink'
    if not stat.S_ISREG(info.st_mode):
        return 'not_regular'
    return None if info.st_size == size else 'size_mismatch'


def migrate(db, manifest, library='local-library'):
    counts = collections.Counter()
    for table in ('catalog_paths', 'jobsdb.files', 'jobsdb.folders', 'jobsdb.jobs'):
        column = 'current_path' if table == 'jobsdb.jobs' else 'path'
        for rowid, old in db.execute(f'SELECT rowid,{column} FROM {table} WHERE {column} IS NOT NULL').fetchall():
            new = mapped(old)
            if new != old:
                # Constraint conflicts must abort the transaction, never replace associations.
                db.execute(f'UPDATE {table} SET {column}=? WHERE rowid=?', (new, rowid))
                counts['migrated_' + table] += 1
    for item in manifest:
        path, asset, digest, role, size = (item[key] for key in ('path', 'asset_id', 'content_hash', 'role', 'size'))
        if not all(isinstance(value, str) and value for value in (path, asset, digest, role)) or type(size) is not int or size < 0:
            raise ValueError('Invalid manifest record')
        if item.get('library_id', library) != library:
            raise ValueError('Manifest library mismatch')
        reason = checked_file(path, size)
        if reason:
            counts['skipped_' + reason] += 1
            continue
        existing = db.execute('SELECT asset_id,content_hash,role FROM catalog_paths WHERE library_id=? AND path=?', (library, path)).fetchone()
        if existing and existing != (asset, digest, role):
            raise ValueError('Conflicting catalog association: ' + path)
        matches = db.execute('SELECT EXISTS(SELECT 1 FROM catalog_assets a JOIN catalog_files f ON f.library_id=a.library_id AND f.asset_id=a.id WHERE a.library_id=? AND a.id=? AND f.content_hash=? AND f.role=? AND f.size=?)', (library, asset, digest, role, size)).fetchone()[0]
        if not matches:
            counts['skipped_catalog_mismatch'] += 1
            continue
        if existing:
            counts['already_present'] += 1
        else:
            db.execute('INSERT INTO catalog_paths(library_id,path,asset_id,content_hash,role) VALUES(?,?,?,?,?)', (library, path, asset, digest, role))
            counts['restored'] += 1
    return dict(counts)


def run(catalog, jobs, manifest, apply=False, backup_dir=None):
    scratch = tempfile.TemporaryDirectory(prefix='keeps-path-dry-run-')
    sources = [sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True) for path in (catalog, jobs)]
    try:
        for source in sources:
            integrity(source)
        if apply:
            if backup_dir is None:
                raise ValueError('--backup-dir is required with --apply')
            destination = Path(backup_dir) / datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
            destination.mkdir(parents=True, exist_ok=False)
            for source, name in zip(sources, ('control_plane.sqlite', 'jobs.sqlite')):
                with closing(sqlite3.connect(destination / name)) as backup, backup:
                    source.backup(backup)
                    integrity(backup)
            db = sqlite3.connect(catalog)
            db.execute('ATTACH DATABASE ? AS jobsdb', (str(jobs),))
        else:
            copies = [Path(scratch.name) / name for name in ('catalog.sqlite', 'jobs.sqlite')]
            for source, copy in zip(sources, copies):
                with closing(sqlite3.connect(copy)) as target, target:
                    source.backup(target)
            db = sqlite3.connect(copies[0])
            db.execute('ATTACH DATABASE ? AS jobsdb', (str(copies[1]),))
        try:
            db.execute('BEGIN IMMEDIATE')
            counts = migrate(db, manifest)
            integrity(db)
            integrity(db, 'jobsdb')
            db.commit()
            return {'mode': 'apply' if apply else 'dry_run', 'counts': counts, 'backup': str(destination) if apply else None, 'content_hash_reverified': False}
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()
    finally:
        for source in sources:
            source.close()
        scratch.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog', default='/volume2/myphoto/keeps/db/control_plane.sqlite')
    parser.add_argument('--jobs', default='/volume2/myphoto/keeps/db/jobs.sqlite')
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--apply', action='store_true', help='Caller must stop Keeps first')
    parser.add_argument('--backup-dir')
    args = parser.parse_args()
    with open(args.manifest, encoding='utf-8') as source:
        manifest = json.load(source)
    if not isinstance(manifest, list):
        raise ValueError('Manifest must be a JSON array')
    print(json.dumps(run(args.catalog, args.jobs, manifest, args.apply, args.backup_dir), ensure_ascii=False))


if __name__ == '__main__':
    main()
