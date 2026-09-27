from contextlib import closing
import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('migration', Path(__file__).with_name('migrate_nas_paths.py'))
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


class MigrationTests(unittest.TestCase):
    def test_restore_migration_dry_run_and_conflict_rollback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            originals = root / 'photos'
            originals.mkdir()
            photo = originals / 'photo.jpg'
            photo.write_bytes(b'original')
            symlink = originals / 'linked.jpg'
            symlink.symlink_to(photo)
            catalog, jobs = root / 'catalog.sqlite', root / 'jobs.sqlite'
            with closing(sqlite3.connect(catalog)) as db, db:
                db.executescript('''CREATE TABLE catalog_assets(library_id TEXT,id TEXT);
                CREATE TABLE catalog_files(library_id TEXT,asset_id TEXT,content_hash TEXT,role TEXT,size INTEGER,holder TEXT);
                CREATE TABLE catalog_paths(library_id TEXT,path TEXT,asset_id TEXT,content_hash TEXT,role TEXT,PRIMARY KEY(library_id,path));
                INSERT INTO catalog_assets VALUES('local-library','asset');
                INSERT INTO catalog_files VALUES('local-library','asset','hash','jpeg_original',8,'old-mac');
                INSERT INTO catalog_paths VALUES('local-library','/originals/library/existing.jpg','asset','hash','jpeg_original');''')
            with closing(sqlite3.connect(jobs)) as db, db:
                db.executescript('''CREATE TABLE folders(path TEXT UNIQUE); CREATE TABLE files(path TEXT UNIQUE); CREATE TABLE jobs(current_path TEXT);
                INSERT INTO folders VALUES('/originals'); INSERT INTO files VALUES('/originals/library/existing.jpg'); INSERT INTO jobs VALUES('/originals/library/existing.jpg');''')
            item = dict(library_id='local-library', path=str(photo), asset_id='asset', content_hash='hash', role='jpeg_original', size=8)
            manifest = [item, dict(item, path=str(symlink)), dict(item, path=str(originals / 'missing.jpg')), dict(item, size=9)]
            with patch.object(migration, 'MAPPINGS', (('/originals/library', str(originals)),)):
                report = migration.run(catalog, jobs, manifest)
                self.assertEqual(report['counts']['restored'], 1)
                self.assertEqual(report['counts']['skipped_symlink'], 1)
                self.assertEqual(report['counts']['skipped_missing'], 1)
                self.assertEqual(report['counts']['skipped_size_mismatch'], 1)
                with closing(sqlite3.connect(catalog)) as db, db:
                    self.assertEqual(db.execute('SELECT path FROM catalog_paths').fetchone()[0], '/originals/library/existing.jpg')
                # Conflict after earlier mutations rolls both databases back.
                with self.assertRaises(ValueError):
                    migration.run(catalog, jobs, [item, dict(item, asset_id='different')], True, root / 'backups')
                with closing(sqlite3.connect(jobs)) as db, db:
                    self.assertEqual(db.execute('SELECT path FROM files').fetchone()[0], '/originals/library/existing.jpg')
                report = migration.run(catalog, jobs, manifest, True, root / 'backups')
                self.assertTrue((Path(report['backup']) / 'control_plane.sqlite').exists())
                with closing(sqlite3.connect(catalog)) as db, db:
                    self.assertEqual(db.execute('SELECT count(*) FROM catalog_paths').fetchone()[0], 2)
                    self.assertEqual(db.execute('SELECT holder FROM catalog_files').fetchone()[0], 'old-mac')
                with closing(sqlite3.connect(jobs)) as db, db:
                    self.assertEqual(db.execute('SELECT path FROM files').fetchone()[0], str(originals / 'existing.jpg'))
                report = migration.run(catalog, jobs, [item])
                self.assertEqual(report['counts']['already_present'], 1)
                self.assertEqual(photo.read_bytes(), b'original')


if __name__ == '__main__':
    unittest.main()
