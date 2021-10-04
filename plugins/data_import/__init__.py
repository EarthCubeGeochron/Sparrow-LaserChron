from click import command, option, argument, secho
from sparrow.plugins import SparrowPlugin
from sparrow.ext import CloudDataPlugin
from sparrow.import_helpers import SparrowImportError
from sparrow.cli.util import with_app, with_database
from starlette.exceptions import HTTPException
from starlette.responses import Response
from textwrap import wrap
import sparrow

from .extract_datatable import extract_s3_object
from .laserchron_importer import LaserchronImporter, decode_datatable
from .sample_names import list_sample_names
from .cli import import_laserchron


def data_file_csv(request):
    """A route to get CSV data for this specific data file"""
    # Need to lock this method down with authentication...
    uuid = request.path_params["uuid"]

    db = sparrow.get_database()
    DataFile = db.model.data_file

    datafile = db.session.query(DataFile).get(uuid)
    if datafile is None:
        raise HTTPException(404, f"Data file with UUID {uuid} not found")
    if datafile.csv_data is None:
        raise HTTPException(
            404, f"Data file {uuid} does not have an extracted CSV table"
        )

    df = decode_datatable(datafile.csv_data)[0].sort_index().reset_index()
    return Response(df.to_json(orient="records"), media_type="application/json")


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

    def process_objects(self, only_untracked=True):
        """Download all cloud data objects."""
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

    def on_setup_cli(self, cli):
        cli.add_command(import_laserchron)

    def on_api_initialized_v2(self, api):
        api.add_route(
            "/data_file/{uuid}/csv_data",
            data_file_csv,
            help="CSV data for a detrital zircon data table",
        )


@sparrow.task()
def list_samples(verbose: bool = False):
    print("Starting to list samples")
    db = sparrow.get_database()
    DataFile = db.model.data_file
    iterator = db.session.query(DataFile).filter(DataFile.csv_data != None)
    list_sample_names(iterator, verbose=verbose)
