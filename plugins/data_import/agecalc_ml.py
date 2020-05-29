from sys import exit
from os import environ, listdir, path
from datetime import datetime
from click import command, option, echo, secho, style
from IPython import embed
from h5py import File

from sparrow.database import get_or_create
from sparrow.util import relative_path

def import_agecalc_ml(test=True):
    """
    Import Matlab save file for E2 in bulk.
    """
    if not test:
        echo(f"Only test data supported for now")
        return
    fn = relative_path(__file__,'../test-data/Test_E2_Export.mat')
    print(fn)
    # Load file as a pre-7.3 matlab file
    mat = loadmat(fn)
    embed()
