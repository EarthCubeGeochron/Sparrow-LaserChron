from io import StringIO, BytesIO, IOBase
from os import stat
from pandas import read_excel
from xlrd import open_workbook, XLRDError
from click import secho
from uuid import UUID
from sqlalchemy.dialects.postgresql import insert
from pathlib import Path
from datetime import datetime

from sparrow.import_helpers import SparrowImportError, md5hash

def get_excel_reader(infile):
    try:
        if isinstance(infile, IOBase):
            # We have an in-memory file
            fc = infile.read()
            return open_workbook(file_contents=fc, on_demand=True)
        else:
            # We have a filename string
            return open_workbook(infile, on_demand=True)
    except Exception as err:
        raise SparrowImportError(str(err))


def encode_datatable(infile):
    try:
        wb = get_excel_reader(infile)
        df = read_excel(wb, sheet_name="datatable", header=None)
    except XLRDError:
        if "AGE PICK" in infile.stem:
            raise NotImplementedError("AGE PICK files are not yet handled")
        raise SparrowImportError("No data table")
    except AssertionError:
        raise SparrowImportError("Could not open data table")

    b = StringIO()
    df.to_csv(b, compression='gzip')
    b.seek(0)
    # Convert to a binary representation
    return b.read().encode()


def insert_on_conflict_update(db, model, **cols):
    tbl = model.__table__
    index_elements = cols.pop('_index_elements', None)
    if index_elements is None:
        index_elements = model.__mapper__.primary_key

    ix_names = [e.name for e in index_elements]
    non_ix_cols = {k: cols[k] for k in cols if k not in ix_names}

    sql = (insert(tbl)
           .values(**cols)
           .on_conflict_do_update(
                index_elements=index_elements,
                set_=non_ix_cols))

    db.session.execute(sql)


def import_datafile(db, infile):
    """
    Import the `datafile` Excel sheet to a CSV representation
    stored within the database that can be further processed without
    expensive filesystem operations. This also stores the original file's
    hash in order to avoid importing unchanged data.

    Returns boolean (whether file was imported).
    """
    res = stat(infile)
    mtime = datetime.utcfromtimestamp(res.st_mtime)

    hash = md5hash(infile)

    data_file = db.model.data_file

    # Should maybe make sure error is not set
    rec = db.get(data_file, hash)
    # We are done if we've already imported
    if rec is not None:
        secho("Already downloaded")
        return False

    # Values to insert
    cols = dict(
        file_hash=hash,
        file_mtime=mtime,
        basename=infile.stem,
        csv_data=None)

    try:
        cols['csv_data'] = encode_datatable(infile)
    except NotImplementedError as e:
        secho(str(e), fg='red', dim=True)

    insert_on_conflict_update(db, data_file, **cols)
    return True


def extract_s3_object(db, meta, content, redo=False):
    # For some reason the ETag comes wrapped in quotes
    etag = meta['ETag'].replace('"', "")
    # S3 works in terms of 'keys' instead of filenames
    key = meta["Key"]

    secho(str(key), dim=True)

    fobj = BytesIO(content.read())
    fobj.stem = Path(key).stem
    # The md5 hash of a file is not always equivalent to its "ETag",
    # but often is...
    file_hash = md5hash(fobj)

    data_file = db.model.data_file

    # Should maybe make sure error is not set
    rec = db.get(data_file, file_hash)
    # We are done if we've already imported
    if rec is not None and not redo:
        secho("Already extracted", fg='green', dim=True)
        return rec, False

    # Values to insert
    cols = dict(
        file_path=key,
        file_hash=file_hash,
        file_etag=etag,
        file_mtime=meta['LastModified'],
        basename=fobj.stem,
        csv_data=None)

    try:
        cols['csv_data'] = encode_datatable(fobj)
    except (SparrowImportError, NotImplementedError, IndexError, UnicodeDecodeError) as e:
        secho(str(e), fg='red', dim=True)

    insert_on_conflict_update(db, data_file, **cols)
    # Make sure we have updated values
    db.session.flush()
    return rec, True
