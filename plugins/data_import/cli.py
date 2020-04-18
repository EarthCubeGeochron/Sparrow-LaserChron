#!/usr/bin/env python

from os import environ
from click import command, option, argument, echo, secho, style
from pathlib import Path
from sparrow import Database
from sparrow.import_helpers import SparrowImportError, working_directory
from itertools import chain

from .extract_datatable import import_datafile
from .laserchron_importer import LaserchronImporter

def extract_data(db, stop_on_error=False):
    path = Path('.')
    files = chain(path.glob("**/*.xls"), path.glob("**/*.xls[xm]"))
    for f in files:
        try:
            secho(str(f), dim=True)
            imported = import_datafile(db, f)
            db.session.commit()
            if not imported:
                secho("Already imported", fg='green', dim=True)
        except (SparrowImportError, NotImplementedError) as e:
            if stop_on_error: raise e
            db.session.rollback()
            secho(str(e), fg='red')
