from click import command, option, argument
from sparrow.cli.util import with_app

# Right now the command-line application is relatively
# loosely coupled to the importer plugin, which is probably
# good overall but seems a bit awkward.
#
# We might want to implement a 'sparrow.plugin_command' decorator
# in Sparrow's core to make this a bit easier to work with


@command(name="import-laserchron")
@option("--stop-on-error", is_flag=True, default=False)
@option("--verbose", "-v", is_flag=True, default=False)
@option("--download/--no-download", default=True)
@option("--normalize/--no-normalize", default=True)
@option("--redo", default=False, is_flag=True)
@argument("basename", required=False, nargs=-1)
@with_app
def import_laserchron(app, **kwargs):
    """
    Import LaserChron files
    """
    plugin = app.plugins.get("laserchron-data")
    plugin.import_data(**kwargs)
