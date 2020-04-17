from sparrow.plugins.cloud_data import CloudDataPlugin
from sparrow.import_helpers import SparrowImportError
from .extract_datatable import extract_s3_object


class LaserChronDataPlugin(CloudDataPlugin):
    name = "laserchron-data"
    stop_on_error = False

    def import_object(self, meta, obj):
        db = self.app.database
        try:
            extract_s3_object(db, meta, obj)
            db.session.commit()
        except (SparrowImportError, NotImplementedError) as e:
            if self.stop_on_error:
                raise e
            db.session.rollback()
