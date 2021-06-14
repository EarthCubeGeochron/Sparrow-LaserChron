from click import command, option, argument
from sparrow.cli.util import with_app

@command(name="import-laserchron-metadata")
@option('--filename', '--fn', default='alc-06-07-21.csv')
@with_app
def import_laserchron_metadata(app, filename):
    """ 
    import laserchron metadata from downloaded csv
    """

    MetadataImporter = app.plugins.get("laserchron-metadata")
    MetadataImporter.iterfiles(filename)