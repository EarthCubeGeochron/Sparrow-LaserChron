from click import command, option, argument
from sparrow.import_helpers import SparrowImportError

from sparrow.plugins.cloud_data import CloudDataPlugin
from .extract_datatable import extract_s3_object
from .laserchron_importer import LaserchronImporter


class LaserChronDataPlugin(CloudDataPlugin):
    name = "laserchron-data"
    stop_on_error = False
    redo = False

    def import_object(self, meta):
        db = self.app.database
        # Don't download body unless we really need to
        body = None
        inst = self._instance_for_meta(meta)
        if inst is None or self.redo:
            try:
                inst, extracted = extract_s3_object(db, meta, body)
                db.session.commit()
            except (SparrowImportError, NotImplementedError) as e:
                if self.stop_on_error:
                    raise e
                db.session.rollback()
        return inst

    def on_setup_cli(self, cli):
        db = self.app.database

        @command(name="import-laserchron")
        @option('--stop-on-error', is_flag=True, default=False)
        @option('--verbose', '-v', is_flag=True, default=False)
        @option('--download/--no-download', default=True)
        @option('--normalize/--no-normalize', default=True)
        @option('--redo', default=False, is_flag=True)
        @argument('basename', required=False, nargs=-1)
        def cmd(basename=None, stop_on_error=False, verbose=False,
                download=False, normalize=True, redo=False):
            """
            Import LaserChron files
            """

            self.stop_on_error = stop_on_error
            self.redo = redo

            importer = LaserchronImporter(db)
            if normalize and not basename:
                if download:
                    iterator = self.process_objects(only_untracked=False)
                else:
                    # Just use files that are already tracked in the data files object
                    iterator = db.session.query(self.db.model.data_file)
                importer.iter_records(iterator, redo=redo)
            elif basename:
                importer.import_one(basename)

        cli.add_command(cmd)
