from click import command, option, argument, secho
from sparrow.plugins import SparrowPlugin
from sparrow.ext import CloudDataPlugin
from sparrow.import_helpers import SparrowImportError
from sparrow.cli.util import with_app, with_database
from textwrap import wrap

from .extract_datatable import extract_s3_object
from .laserchron_importer import LaserchronImporter
from .sample_names import list_sample_names
from .cli import import_laserchron, list_samples


class LaserChronDataPlugin(SparrowPlugin):

    name = "laserchron-data"
    dependencies = ["cloud-data"]
    stop_on_error = False
    redo = False

    def import_object(self, meta):
        db = self.app.database
        # Don't download body unless we really need to
        body = None
        inst = self.cloud._instance_for_meta(meta)
        if inst is None or self.redo:
            try:
                body = self.cloud.get_body(meta["Key"])
                # Extract s3 object to a CSV file
                inst, extracted = extract_s3_object(db, meta, body, redo=self.redo)
                db.session.commit()
            except (SparrowImportError, NotImplementedError) as e:
                if self.stop_on_error:
                    raise e
                db.session.rollback()
        return inst

    def process_objects(self, only_untracked=True, verbose=False):
        self.cloud = self.app.plugins.get("cloud-data")
        for obj in self.cloud.iterate_objects(only_untracked=only_untracked):
            yield self.import_object(obj)

    def import_data(
        self,
        basename=None,
        stop_on_error=False,
        download=False,
        normalize=True,
        redo=False,
        verbose=False,
    ):
        """
        Import LaserChron files
        """
        db = self.app.database
        db.session.rollback()

        self.stop_on_error = stop_on_error
        self.redo = redo

        importer = LaserchronImporter(self.app, verbose=verbose)
        secho("Starting import")

        if normalize and not basename:
            if download:
                iterator = self.process_objects(only_untracked=False)
            else:
                # Just use files that are already tracked in the data files object
                iterator = db.session.query(db.model.data_file)
            importer.iter_records(iterator, redo=redo)
        elif basename:
            importer.import_one(basename)
        else:
            list(self.process_objects(only_untracked=True, verbose=True))

    def list_samples(self, verbose=False):
        db = self.app.database
        importer = LaserchronImporter(self.app)
        data_file = db.model.data_file
        iterator = db.session.query(data_file).filter(data_file.csv_data != None)
        list_sample_names(iterator, verbose=verbose)

    def on_setup_cli(self, cli):
        cli.add_command(import_laserchron)
        cli.add_command(list_samples)

    def on_register_tasks(self, mgr):
        importer = LaserchronImporter(self.app)
        mgr.register_task(importer.id, importer)
