from click import command, option, secho
from sparrow.cli.util import with_app
from sparrow.task_manager import task
import sparrow

@command(name="import-laserchron-metadata")
@option('--filename', '--fn', default='alc_metadata.csv')
@with_app
def import_laserchron_metadata(app, filename):
    """ 
    import laserchron metadata from downloaded csv
    """

    MetadataImporter = app.plugins.get("laserchron-metadata")
    MetadataImporter.iterfiles(filename)

@task(name="import-laserchron-metadata")
def import_laserchron_metdata_(filename:str = "alc_metadata.csv"):
    """
    importer as a task
    """
    MetadataImporter = sparrow.get_plugin("laserchron-metadata")
    MetadataImporter.iterfiles(filename)

@task(name="say-hello")
def say_hello_task():
    """ 
    Very basic example task
    """
    secho("Hello World", fg="green")