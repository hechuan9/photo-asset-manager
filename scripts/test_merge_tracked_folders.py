#!/usr/bin/env python3
"""Temporary SQLite and synthetic-file acceptance tests; no NAS access."""
import contextlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import merge_tracked_folders as merge

CATALOG = '''
PRAGMA user_version=3;
CREATE TABLE catalog_assets(library_id TEXT NOT NULL,id TEXT NOT NULL,snapshot TEXT NOT NULL,content_hash TEXT NOT NULL,fingerprint TEXT NOT NULL,sort_time TEXT NOT NULL,filename TEXT NOT NULL,rating INTEGER NOT NULL,flag TEXT NOT NULL,color TEXT,trashed INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(library_id,id));
CREATE TABLE catalog_files(library_id TEXT NOT NULL,asset_id TEXT NOT NULL,content_hash TEXT NOT NULL,size INTEGER NOT NULL,role TEXT NOT NULL,holder TEXT NOT NULL,availability TEXT NOT NULL,PRIMARY KEY(library_id,asset_id,content_hash,role,holder));
CREATE TABLE catalog_paths(library_id TEXT NOT NULL,path TEXT NOT NULL,asset_id TEXT NOT NULL,content_hash TEXT NOT NULL,role TEXT NOT NULL,PRIMARY KEY(library_id,path));
CREATE TABLE catalog_versions(library_id TEXT NOT NULL,asset_id TEXT NOT NULL,content_hash TEXT NOT NULL,visual_hash TEXT,capture_key TEXT,width INTEGER NOT NULL,height INTEGER NOT NULL,priority INTEGER NOT NULL,evidence TEXT NOT NULL,PRIMARY KEY(library_id,asset_id,content_hash));
CREATE TABLE catalog_version_paths(library_id TEXT NOT NULL,path TEXT NOT NULL,asset_id TEXT NOT NULL,content_hash TEXT NOT NULL,available INTEGER NOT NULL,PRIMARY KEY(library_id,path));
CREATE TABLE catalog_defaults(library_id TEXT NOT NULL,asset_id TEXT NOT NULL,content_hash TEXT NOT NULL,user_selected INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(library_id,asset_id));
CREATE TABLE catalog_version_revision(library_id TEXT PRIMARY KEY,revision INTEGER NOT NULL);
CREATE TABLE derivative_objects(library_id VARCHAR NOT NULL,asset_id VARCHAR(36) NOT NULL,role VARCHAR NOT NULL,file_object JSON NOT NULL,object_bucket VARCHAR NOT NULL,object_key VARCHAR NOT NULL,object_etag VARCHAR,pixel_width BIGINT NOT NULL,pixel_height BIGINT NOT NULL,declared_event_seq BIGINT NOT NULL,updated_at DATETIME NOT NULL,PRIMARY KEY(library_id,asset_id,role));
'''
JOBS = '''
CREATE TABLE folders(id TEXT PRIMARY KEY,library_id TEXT NOT NULL,path TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1,UNIQUE(library_id,path));
CREATE TABLE files(folder_id TEXT NOT NULL REFERENCES folders(id),path TEXT NOT NULL,size INTEGER NOT NULL,mtime_ns INTEGER NOT NULL,asset_id TEXT NOT NULL,version TEXT NOT NULL,error TEXT,metadata_stamp TEXT NOT NULL DEFAULT '',PRIMARY KEY(folder_id,path));
'''


@contextlib.contextmanager
def database(path):
    with contextlib.closing(sqlite3.connect(path)) as db:
        with db:
            yield db


class FakeInspector:
    def __init__(self):
        self.visual = {}
        self.calls = []

    def inspect(self, path):
        self.calls.append(path)
        return json.loads(json.dumps(dict(path=str(path), sha256=merge.sha256(path),
            visual_hash=self.visual.get(str(path)), role='jpeg_original', width=10, height=10,
            priority=2, capture=dict(captureTime='2025-01-01T00:00:00Z', cameraMake='test',
                cameraModel='camera', lensModel='lens', cameraSerial='', captureOriginal=''),
            stamp=merge.stamp(path), sidecar_stamp=merge.sidecar_stamp(path))))


class MergeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.keeps = self.root / 'keeps'
        (self.keeps / 'db').mkdir(parents=True)
        self.photos = self.root / 'photos'
        self.photos.mkdir()
        self.run = self.keeps / 'maintenance' / 'run'
        self.catalog = self.keeps / 'db/control_plane.sqlite'
        self.jobs = self.keeps / 'db/jobs.sqlite'
        with database(self.catalog) as db:
            db.executescript(CATALOG)
        with database(self.jobs) as db:
            db.executescript(JOBS)
        self.inspector = FakeInspector()
        self.folder('root', self.photos)

    def folder(self, name, path, active=1):
        path.mkdir(parents=True, exist_ok=True)
        with database(self.jobs) as db:
            db.execute('INSERT INTO folders VALUES(?,?,?,?)', (name, 'library', str(path), active))

    def photo(self, name, asset, content=b'same', folder='root', visual=None, rating=0, selected=False):
        path = self.photos / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        digest = merge.sha256(path)
        snapshot = dict(id=asset, rating=rating, flagState='none', colorLabel=None, caption='', tags=[], trashed=False)
        with database(self.catalog) as db:
            db.execute('INSERT OR IGNORE INTO catalog_assets VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                ('library', asset, json.dumps(snapshot), digest, 'fingerprint', '2025', name, rating, 'none', None, 0))
            db.execute('INSERT OR IGNORE INTO catalog_files VALUES(?,?,?,?,?,?,?)', ('library', asset, digest, len(content), 'jpeg_original', 'nas', 'online'))
            db.execute('INSERT INTO catalog_paths VALUES(?,?,?,?,?)', ('library', str(path), asset, digest, 'jpeg_original'))
            db.execute('INSERT OR IGNORE INTO catalog_versions VALUES(?,?,?,?,?,?,?,?,?)', ('library', asset, digest, visual, None, 10, 10, 2, '{}'))
            db.execute('INSERT INTO catalog_version_paths VALUES(?,?,?,?,?)', ('library', str(path), asset, digest, 1))
            db.execute('INSERT OR IGNORE INTO catalog_defaults VALUES(?,?,?,?)', ('library', asset, digest, int(selected)))
        with database(self.jobs) as db:
            db.execute('INSERT INTO files VALUES(?,?,?,?,?,?,?,?)', (folder, str(path), len(content), path.stat().st_mtime_ns, asset, digest, None, ''))
        self.inspector.visual[str(path)] = visual
        return path

    def make_plan(self):
        with contextlib.redirect_stdout(io.StringIO()):
            return merge.plan(self.keeps, [self.photos], self.run, self.inspector)

    def apply(self):
        with patch.object(merge, 'layout', return_value=(self.keeps, [self.photos], False)):
            return merge.apply(self.run, 'test')

    def dump(self):
        result = []
        for path in [self.catalog, self.jobs]:
            with database(path) as db:
                result.append('\n'.join(db.iterdump()))
        return result

    def groups(self, report):
        return [g for d in report['directories'] for g in d['groups']]

    def test_plan_is_read_only_and_scope_intersection(self):
        self.photo('a.jpg', 'a')
        self.photo('b.jpg', 'b')
        outside = self.root / 'photos-other'
        self.folder('outside', outside)
        (outside / 'outside.jpg').write_bytes(b'outside')
        inactive = self.root / 'inactive'
        self.folder('inactive', inactive, 0)
        (inactive / 'inactive.jpg').write_bytes(b'inactive')
        before = self.dump()
        report = self.make_plan()
        self.assertEqual(before, self.dump())
        self.assertEqual({p.parent for p in self.inspector.calls}, {self.photos})
        self.assertEqual(len(self.groups(report)), 1)
        roots = merge.walk_roots([['ancestor', 'library', str(self.root)], ['inactive', 'other', str(inactive)]], [self.photos])
        self.assertEqual(roots, [('library', self.photos)])

    def test_parent_child_same_prefix_are_separate(self):
        for directory in ['', 'child', 'child-extra']:
            for i in range(2):
                self.photo(str(Path(directory) / f'{i}.jpg'), f'{directory or "parent"}-{i}')
        self.folder('child', self.photos / 'child')
        report = self.make_plan()
        groups = self.groups(report)
        self.assertEqual(len(groups), 3)
        self.assertEqual(len(report['directories']), 3)
        self.assertTrue(all(len(g['members']) == 2 for g in groups))
        self.assertEqual(len(self.inspector.calls), 6)

    def test_cross_directory_asset_bridge_is_skipped(self):
        self.photo('a.jpg', 'a')
        self.photo('b.jpg', 'b')
        self.photo('child/a.jpg', 'a')
        report = self.make_plan()
        self.assertEqual(self.groups(report), [])
        self.assertEqual(report['directories'][0]['skipped'][0]['reason'], 'asset_spans_directories')

    def test_user_metadata_and_user_default_conflicts(self):
        self.photo('ratings/a.jpg', 'a', rating=1)
        self.photo('ratings/b.jpg', 'b', rating=2)
        self.photo('defaults/c.jpg', 'c', b'c', visual='v', selected=True)
        self.photo('defaults/d.jpg', 'd', b'd', visual='v', selected=True)
        report = self.make_plan()
        self.assertEqual(self.groups(report), [])
        reasons = {g['reason'] for d in report['directories'] for g in d['skipped']}
        self.assertEqual(reasons, {'user_metadata_conflict', 'user_default_conflict'})

    def test_metadata_alone_does_not_merge(self):
        self.photo('a.jpg', 'a', b'a')
        self.photo('b.jpg', 'b', b'b')
        report = self.make_plan()
        self.assertEqual(self.groups(report), [])
        self.assertEqual(report['directories'][0]['metadata_candidates'][0]['reason'], 'requires_visual_confirmation')

    def test_apply_preserves_versions_default_and_sources_and_is_idempotent(self):
        paths = [self.photo('a.jpg', 'a', b'a', visual='v'), self.photo('b.jpg', 'b', b'b', visual='v', selected=True)]
        before = {p: p.read_bytes() for p in paths}
        report = self.make_plan()
        self.assertEqual(self.groups(report)[0]['survivor'], 'b')
        result = self.apply()
        self.assertEqual(result['merged_assets'], 1)
        with database(self.catalog) as db:
            self.assertEqual(db.execute('SELECT id FROM catalog_assets').fetchall(), [('b',)])
            self.assertEqual(db.execute('SELECT count(*) FROM catalog_versions').fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT DISTINCT asset_id FROM catalog_version_paths').fetchall(), [('b',)])
            self.assertEqual(db.execute('SELECT content_hash,user_selected FROM catalog_defaults').fetchone(), (merge.sha256(paths[1]), 1))
        with database(self.jobs) as db:
            self.assertEqual(db.execute('SELECT DISTINCT asset_id FROM files').fetchall(), [('b',)])
        once = self.dump()
        self.assertTrue(self.apply()['already_applied'])
        self.assertEqual(once, self.dump())
        self.assertEqual(before, {p: p.read_bytes() for p in paths})
        self.assertTrue((Path(result['backup']) / 'jobs.sqlite').is_file())

    def test_same_hash_apply_keeps_every_path(self):
        self.photo('a.jpg', 'a')
        self.photo('b.jpg', 'b')
        self.make_plan()
        self.apply()
        with database(self.catalog) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM catalog_assets').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM catalog_versions').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM catalog_paths').fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT count(*) FROM catalog_version_paths').fetchone()[0], 2)

    def test_changed_catalog_or_tracking_rejects_apply(self):
        self.photo('a.jpg', 'a')
        self.photo('b.jpg', 'b')
        self.make_plan()
        with database(self.catalog) as db:
            db.execute("UPDATE catalog_assets SET snapshot=json_set(snapshot,'$.rating',5) WHERE id='a'")
        changed = self.dump()
        with self.assertRaisesRegex(ValueError, '数据库'):
            self.apply()
        self.assertEqual(changed, self.dump())
        with database(self.jobs) as db:
            db.execute('UPDATE folders SET active=0')
        with self.assertRaisesRegex(ValueError, '追踪配置'):
            self.apply()

    def test_apply_requires_stopped_container(self):
        self.photo('a.jpg', 'a')
        self.photo('b.jpg', 'b')
        self.make_plan()
        before = self.dump()
        with patch.object(merge, 'layout', return_value=(self.keeps, [self.photos], True)):
            with self.assertRaisesRegex(ValueError, '停止'):
                merge.apply(self.run, 'test')
        self.assertEqual(before, self.dump())

    def test_mid_transaction_failure_rolls_back_both_databases(self):
        for directory in ['one', 'two']:
            self.photo(directory + '/a.jpg', directory + '-a')
            self.photo(directory + '/b.jpg', directory + '-b')
        self.make_plan()
        before = self.dump()
        original = merge.apply_group
        calls = []
        def fail_after_mutation(db, group):
            original(db, group)
            calls.append(group)
            if len(calls) == 2:
                raise RuntimeError('injected after both databases changed')
        with patch.object(merge, 'apply_group', side_effect=fail_after_mutation):
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                self.apply()
        self.assertEqual(before, self.dump())
        with database(self.run / 'applied.sqlite') as db:
            self.assertEqual(db.execute('SELECT count(*) FROM runs').fetchone()[0], 0)

    def test_sidecar_only_change_rejects_apply(self):
        path = self.photo('a.jpg', 'a')
        self.photo('b.jpg', 'b')
        sidecar = path.with_suffix('.xmp')
        sidecar.write_bytes(b'initial xmp')
        self.make_plan()
        before = self.dump()
        original_stamp = merge.stamp(path)
        sidecar.write_bytes(b'changed xmp metadata')
        with self.assertRaisesRegex(ValueError, 'sidecar'):
            self.apply()
        self.assertEqual(before, self.dump())
        self.assertEqual(original_stamp, merge.stamp(path))

    def test_resume_reinspects_after_sidecar_removed(self):
        path = self.photo('a.jpg', 'a')
        other = self.photo('b.jpg', 'b')
        sidecar = path.with_suffix('.xmp')
        sidecar.write_bytes(b'initial xmp')
        self.make_plan()
        self.inspector.calls.clear()
        sidecar.unlink()
        with contextlib.redirect_stdout(io.StringIO()):
            report = merge.plan(self.keeps, [self.photos], self.run, self.inspector, resume=True)
        self.assertEqual(self.inspector.calls, [path])
        self.assertNotIn(other, self.inspector.calls)
        evidence = self.groups(report)[0]['evidence']
        self.assertTrue(all(f['sidecar_stamp'] == '' for f in evidence))

    def test_interrupted_resume_cannot_apply_previous_plan(self):
        self.photo('a.jpg', 'a')
        self.photo('b.jpg', 'b')
        old_plan = self.make_plan()
        before = self.dump()
        with patch.object(merge, 'copy_database', side_effect=RuntimeError('interrupted snapshot')):
            with self.assertRaisesRegex(RuntimeError, 'interrupted'):
                merge.plan(self.keeps, [self.photos], self.run, self.inspector, resume=True)
        self.assertFalse((self.run / 'plan.json').exists())
        self.assertEqual(json.loads((self.run / 'previous-plan.json').read_text())['id'], old_plan['id'])
        with self.assertRaises(FileNotFoundError):
            self.apply()
        self.assertEqual(before, self.dump())

    def assert_missing_default_is_skipped(self, remove_from):
        self.photo('a.jpg', 'a')
        self.photo('b.jpg', 'b')
        missing = self.photo('edited.jpg', 'a', b'edited-default')
        digest = merge.sha256(missing)
        with database(self.catalog) as db:
            db.execute('UPDATE catalog_versions SET priority=3 WHERE content_hash=?', (digest,))
            db.execute("UPDATE catalog_defaults SET content_hash=?,user_selected=1 WHERE asset_id='a'", (digest,))
            # Exercise each source of required paths independently.
            db.execute('DELETE FROM ' + remove_from + ' WHERE path=?', (str(missing),))
        missing.unlink()
        before = self.dump()
        report = self.make_plan()
        self.assertEqual(self.groups(report), [])
        self.assertEqual(report['directories'][0]['skipped'][0]['reason'], 'indexed_original_missing_or_unverified')
        self.assertEqual(self.apply()['merged_assets'], 0)
        self.assertEqual(before, self.dump())
        with database(self.catalog) as db:
            self.assertEqual(db.execute("SELECT content_hash,user_selected FROM catalog_defaults WHERE asset_id='a'").fetchone(), (digest, 1))
            self.assertEqual(db.execute('SELECT count(*) FROM catalog_assets').fetchone()[0], 2)

    def test_missing_catalog_original_blocks_merge(self):
        self.assert_missing_default_is_skipped('catalog_version_paths')

    def test_missing_available_version_blocks_merge(self):
        self.assert_missing_default_is_skipped('catalog_paths')

    def test_changed_original_rejects_apply(self):
        path = self.photo('a.jpg', 'a')
        self.photo('b.jpg', 'b')
        self.make_plan()
        before = self.dump()
        path.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, '原片'):
            self.apply()
        self.assertEqual(before, self.dump())


if __name__ == '__main__':
    unittest.main()
