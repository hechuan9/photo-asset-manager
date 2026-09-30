import json
import errno
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock
from types import SimpleNamespace

import migrate_nas_storage as migration


class MigrationTests(unittest.TestCase):
    def test_copy_never_overwrites_and_verified_retry_is_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / 'source', Path(directory) / 'target'
            source.write_bytes(b'generated')
            migration.copy_verified(source, target)
            migration.copy_verified(source, target)
            target.write_bytes(b'original')
            with self.assertRaises(RuntimeError):
                migration.copy_verified(source, target)
            self.assertEqual(target.read_bytes(), b'original')
            self.assertEqual(source.read_bytes(), b'generated')

    def test_cache_manifest_skips_verified_unchanged_content(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, destination = root / 'old', root / 'new'
            (source / 'previews').mkdir(parents=True)
            destination.mkdir()
            (source / 'previews' / 'thumbnail').write_bytes(b'cache')
            args = SimpleNamespace(source=source, destination=destination)
            self.assertEqual(migration.copy_cache(args), 1)
            with mock.patch.object(migration, 'digest', side_effect=AssertionError('unexpected rehash')):
                self.assertEqual(migration.copy_cache(args), 1)
            (destination / 'previews' / 'thumbnail').write_bytes(b'bad cache')
            with self.assertRaises(RuntimeError):
                migration.copy_cache(args)

    def test_clone_success_uses_independent_inode_without_hashing(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / 'source', Path(directory) / 'target'
            source.write_bytes(b'cloned content')
            def ioctl(destination_fd, request, source_fd):
                self.assertEqual(request, 0x40049409)
                os.write(destination_fd, os.read(source_fd, 1024))
            with mock.patch.object(migration.sys, 'platform', 'linux'), mock.patch.object(migration.fcntl, 'ioctl', side_effect=ioctl), mock.patch.object(migration, 'digest', side_effect=AssertionError('clone must not hash')):
                migration.copy_verified(source, target)
            self.assertNotEqual(source.stat().st_ino, target.stat().st_ino)
            self.assertEqual(target.read_bytes(), b'cloned content')
            target.write_bytes(b'changed')
            self.assertEqual(source.read_bytes(), b'cloned content')

    def test_unsupported_clone_falls_back_to_verified_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / 'source', Path(directory) / 'target'
            source.write_bytes(b'fallback content')
            with mock.patch.object(migration.sys, 'platform', 'linux'), mock.patch.object(migration.fcntl, 'ioctl', side_effect=OSError(errno.EOPNOTSUPP, 'unsupported')), mock.patch.object(migration, 'digest', wraps=migration.digest) as hashes:
                migration.copy_verified(source, target)
                self.assertEqual(hashes.call_count, 2)
            self.assertEqual(target.read_bytes(), source.read_bytes())
            self.assertNotEqual(source.stat().st_ino, target.stat().st_ino)

    def test_registration_keeps_asset_and_user_selected_default(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'raw.keeps-hash.heic'
            target.write_bytes(b'generated')
            db = sqlite3.connect(':memory:')
            db.executescript('''
CREATE TABLE catalog_assets(library_id,id,snapshot);
INSERT INTO catalog_assets VALUES('lib','asset','{"rating":4}');
CREATE TABLE catalog_paths(library_id,path,asset_id,content_hash,role,PRIMARY KEY(library_id,path));
CREATE TABLE catalog_files(library_id,asset_id,content_hash,size,role,holder,availability,PRIMARY KEY(library_id,asset_id,content_hash,role,holder));
CREATE TABLE catalog_versions(library_id,asset_id,content_hash,visual_hash,capture_key,width,height,priority,evidence,PRIMARY KEY(library_id,asset_id,content_hash));
CREATE TABLE catalog_version_paths(library_id,path,asset_id,content_hash,available,PRIMARY KEY(library_id,path));
CREATE TABLE catalog_defaults(library_id,asset_id,content_hash,user_selected,PRIMARY KEY(library_id,asset_id));
CREATE TABLE media_cache(library_id,asset_id,standard,source_hash,status);
INSERT INTO catalog_defaults VALUES('lib','asset','chosen',1);
INSERT INTO catalog_paths VALUES('lib','/originals/raw.arw','asset','rawhash','raw_original');
INSERT INTO media_cache VALUES('lib','asset',NULL,'rawhash','ready');
''')
            item = dict(library='lib', asset='asset', sourceHash='rawhash', source='/originals/raw.arw', target=str(target), standard=dict(version=migration.digest(target), width=100, height=80))
            migration.register(db, item)
            migration.register(db, item)
            self.assertEqual(db.execute('SELECT count(*) FROM catalog_paths').fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT content_hash FROM catalog_defaults').fetchone()[0], 'chosen')
            descriptor = json.loads(db.execute('SELECT standard FROM media_cache').fetchone()[0])
            self.assertIsInstance(descriptor['mtimeNs'], str)
            self.assertEqual(db.execute('SELECT asset_id FROM catalog_version_paths').fetchone()[0], 'asset')
            self.assertEqual(json.loads(db.execute('SELECT evidence FROM catalog_versions').fetchone()[0])['generatedFrom'], 'rawhash')
            # A pre-version-index asset previously used catalog_assets.content_hash.
            # Adding a generated standard must not silently change that effective default.
            db.execute('DELETE FROM catalog_defaults')
            db.execute("DELETE FROM catalog_versions WHERE content_hash='rawhash'")
            db.execute("DELETE FROM catalog_version_paths WHERE content_hash='rawhash'")
            migration.register(db, item)
            self.assertEqual(db.execute('SELECT content_hash,user_selected FROM catalog_defaults').fetchone(), ('rawhash', 0))
            self.assertEqual(db.execute("SELECT c.status FROM media_cache c JOIN catalog_defaults d USING(library_id,asset_id) WHERE c.source_hash=d.content_hash AND c.status='ready'").fetchone(), ('ready',))
            self.assertEqual(db.execute("SELECT width,height,evidence FROM catalog_versions WHERE content_hash='rawhash'").fetchone(), (0, 0, json.dumps({'capture': {'rating': 4}})))
            self.assertEqual(db.execute("SELECT asset_id,available FROM catalog_version_paths WHERE content_hash='rawhash'").fetchone(), ('asset', 1))
            # Repair only the bad auto-default introduced by the earlier migration.
            db.execute('UPDATE catalog_defaults SET content_hash=?', (item['standard']['version'],))
            migration.register(db, item)
            self.assertEqual(db.execute('SELECT content_hash FROM catalog_defaults').fetchone()[0], 'rawhash')
            db.execute("UPDATE catalog_defaults SET content_hash='another-version'")
            migration.register(db, item)
            self.assertEqual(db.execute('SELECT content_hash FROM catalog_defaults').fetchone()[0], 'another-version')



if __name__ == '__main__':
    unittest.main()
