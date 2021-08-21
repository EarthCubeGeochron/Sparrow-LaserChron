import sparrow
from sparrow.import_helpers import SparrowImportError, BaseImporter
from datetime import datetime
from io import StringIO
from pandas import read_csv, isnull
import numpy as N
from sqlalchemy.exc import IntegrityError, ProgrammingError, DataError
from click import echo, style, secho
from datefinder import find_dates
import re
from time import sleep

from .normalize_data import normalize_data
from .sample_names import generalize_samples


def __extract_datetime(possible_date_string):
    dates = find_dates(possible_date_string, source=True, base_date=datetime.min)
    for date, source_text in dates:
        if len(source_text) < 5:
            continue
        echo(
            "Extracted date "
            + style(str(date), fg="green")
            + " from "
            + style(source_text, fg="green")
        )
        return date

    return None


range_regex = re.compile(r"(\d+)-(\d+)\s")


def extract_datetime(st):
    for pathseg in st.split("/")[::-1]:
        without_ranges = range_regex.sub(r"\2 ", pathseg)
        dt = __extract_datetime(without_ranges)
        if dt is None:
            dt = __extract_datetime(pathseg)
        if dt is not None:
            return dt
    return None


def decode_datatable(csv_data):
    """Extract a CSV data table from its binary representation
    in the PostgreSQL database."""
    tbl = csv_data
    if tbl is None:
        return
    f = StringIO()
    f.write(tbl.decode())
    f.seek(0)
    df = read_csv(f)
    df = df.iloc[:, 1:]
    return normalize_data(df)


def infer_project_name(fp):
    folders = fp.split("/")[:-1]
    return max(folders, key=len)


def nan_to_none(val):
    if isnull(val):
        return None
    return val


def _sample_dataframe(df, sample_name):
    names = df.index.get_level_values("sample_name")
    ix = names == sample_name
    if isnull(sample_name):
        # Special case for null sample name (row only has information about spots)
        ix = names.isnull()
    return df.loc[ix]


class LaserchronImporter(BaseImporter):
    """
    A Sparrow importer for cleaned ETAgeCalc and NUPM AgeCalc files stored
    in the cloud.
    """

    authority = "ALC"
    id = "laserchron-data"
    trust_file_times = False

    def import_all(self, redo=False):
        self.redo = redo
        q = self.db.session.query(self.db.model.data_file)
        self.iter_records(q, redo=redo)

    def import_one(self, basename):
        q = self.db.session.query(self.db.model.data_file).filter_by(basename=basename)
        self.iter_records(q, redo=True)

    def run_task(
        self,
        basename=None,
        stop_on_error=False,
        download=False,
        normalize=True,
        redo=True,
        verbose=False,
    ):
        """
        Import LaserChron files
        """
        db = self.app.database
        db.session.rollback()

        self.stop_on_error = stop_on_error
        self.redo = redo

        if normalize and not basename:
            if download:
                iterator = self.process_objects(only_untracked=False)
            else:
                # Just use files that are already tracked in the data files object
                iterator = db.session.query(db.model.data_file)
            self.iter_records(iterator, redo=redo)
        elif basename:
            self.import_one(basename)
        else:
            list(self.process_objects(only_untracked=True, verbose=True))

    def import_datafile(self, fn, rec, redo=False):
        """
        data file -> sample(s)
        """
        if "NUPM-MON" in rec.basename:
            raise SparrowImportError("NUPM-MON files are not handled yet")
        if not rec.csv_data:
            raise SparrowImportError("CSV data not extracted")

        try:
            data, meta = decode_datatable(rec.csv_data)
            self.meta = meta
            data.index.name = "analysis"
        except IndexError as err:
            raise SparrowImportError(err)

        data = generalize_samples(data)

        sample_names = list(data.index.unique(level=0))

        if self.verbose:
            echo("Samples: " + ", ".join(sample_names))

        for sample_name in sample_names:
            df = _sample_dataframe(data, sample_name)
            try:
                yield self.import_session(rec, df)
            except (IntegrityError, ProgrammingError, DataError) as err:
                raise SparrowImportError(str(err.orig))
            # Handle common error types
            except (IndexError, ValueError, AssertionError, TypeError) as err:
                raise SparrowImportError(err)

    def import_session(self, rec, df):

        # Infer project name
        project_name = infer_project_name(rec.file_path)
        project = self.project(project_name)

        date = extract_datetime(rec.file_path)

        if self.trust_file_times:
            date = rec.file_mtime
        if date is None:
            # Dates are required, but we might change this
            date = datetime.min

        sample_name = nan_to_none(df.index.unique(level="sample_name")[0])
        sample_id = None
        if sample_name is not None:
            sample = self.sample(name=sample_name)
            self.db.session.add(sample)
            sample_id = sample.id

        self.db.session.add(project)

        # See if a matching session exists, otherwise create.
        # Not sure if this will work right now
        session = (
            self.db.session.query(self.m.session)
            .filter(self.m.data_file == rec)
            .first()
        )

        if session is not None:
            self.warn(f"Existing session {session.id} found")
            # Right now we always overwrite Sparrow changes to projects and samples,
            # but this is obviously not appropriate if corrections have been made
            # in the metadata management system.
            # We need to create new sample and project models only if they aren't tied
            # to an existing session, or ask the user whether they want to override
            # Sparrow-configured values by those set within the linked data file.
            session.project_id = project.id
            session.sample_id = sample_id
        else:
            session = self.db.get_or_create(
                self.m.session, project_id=project.id, sample_id=sample_id
            )

        # We always override the date with our estimated value
        session.date = date

        self.db.session.add(session)
        self.db.session.flush()

        dup = df["analysis"].duplicated(keep="first")
        if dup.astype(bool).sum() > 0:
            print(dup)
            self.warn(f"Duplicate analyses found for sample {sample_name}")
        df = df[~dup]

        for i, row in df.iterrows():
            list(self.import_analysis(row, session))

        return session

    def import_analysis(self, row, session):
        """
        row -> analysis
        """
        # session index should not be nan
        try:
            ix = int(row.name[2])
        except ValueError:
            ix = None

        analysis = self.add_analysis(
            session, session_index=ix, analysis_name=str(row.name[1])
        )

        for i in row.iteritems():
            try:
                d = self.import_datum(analysis, *i, row)
            except (ValueError, AttributeError) as err:
                # Correct thing to do: raise SparrowImportError.
                # This tells the application that you explicitly handled
                # this error, and to report it without stopping.
                raise SparrowImportError(err)
            if d is None:
                continue
            yield d

    def import_datum(self, analysis, key, value, row):
        """
        Each value in a table row -> datum
        """
        if key == "analysis":
            return None
        if key.endswith("_error"):
            return None
        if key == "best_age":
            # We test for best ages separately, since they
            # must be one of the other ages
            return None

        try:
            value = float(value)
        except ValueError:
            return None
        if isnull(value):
            return None

        m = self.meta[key]
        parameter = m.name

        unit = self.unit(m.at["Unit"]).id

        err = None
        err_unit = None
        try:
            err_ix = key + "_error"
            err = row.at[err_ix]
            i = self.meta[err_ix].at["Unit"]
            err_unit = self.unit(i).id
        except KeyError:
            pass

        is_age = key.startswith("age_")

        datum = self.datum(
            analysis,
            parameter,
            value,
            unit=unit,
            error=err,
            error_unit=err_unit,
            error_metric="2s",
            is_interpreted=is_age,
        )

        if is_age:
            # Test if it is a "best age"
            best_age = float(row.at["best_age"])
            datum.is_accepted = N.allclose(value, best_age)
        return datum


@sparrow.task(name="import-laserchron")
def import_laserchron(
    basename: str = None,
    stop_on_error: bool = False,
    verbose: bool = False,
    download: bool = False,
    normalize: bool = True,
    redo: bool = False,
):
    """
    Import LaserChron files
    """
    echo("Starting import task")
    plugin = sparrow.get_plugin("laserchron-data")
    print(plugin)
    secho("Test", fg="red")
    plugin.import_data(
        basename,
        stop_on_error=stop_on_error,
        verbose=verbose,
        download=download,
        normalize=normalize,
        redo=redo,
    )
