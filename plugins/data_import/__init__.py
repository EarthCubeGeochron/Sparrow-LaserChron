from sparrow.plugins.cloud_data import CloudDataPlugin
from sparrow.import_helpers import SparrowImportError
from click import secho
from .extract_datatable import extract_s3_object


class LaserChronDataPlugin(CloudDataPlugin):
    name = "laserchron-data"
    stop_on_error = False

    def import_object(self, meta, obj):
        db = self.app.database
        try:
            secho(str(meta["Key"]), dim=True)
            extracted = extract_s3_object(db, meta, obj)
            db.session.commit()
            if not extracted:
                secho("Already extracted", fg='green', dim=True)
        except (SparrowImportError, NotImplementedError) as e:
            if self.stop_on_error:
                raise e
            db.session.rollback()
            secho(str(e), fg='red')
