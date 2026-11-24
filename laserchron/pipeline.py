"""
AgeCalc data tables to Sparrow records.

One workbook holds one run. Each sample in it becomes a session, each spot an
analysis, and each column a datum. The project and the date of a run are not in
the workbook: they are read from the folders it is filed under.
"""

import re
from datetime import datetime
from math import isclose
from pathlib import Path, PurePath

from datefinder import find_dates
from pandas import isnull
from sparrow.pipelines import (
    Analysis,
    Datum,
    DatumType,
    Pipeline,
    Project,
    Sample,
    Session,
)

from .datatable import normalize_data, read_datatable
from .sample_names import DataTableError, generalize_samples


def _extract_datetime(possible_date_string):
    dates = find_dates(possible_date_string, source=True, base_date=datetime.min)
    for date, source_text in dates:
        if len(source_text) < 5:
            continue
        return date
    return None


range_regex = re.compile(r"(\d+)-(\d+)\s")


def extract_datetime(name: str):
    """The date in a file's path, looking from the file name upwards."""
    for pathseg in name.split("/")[::-1]:
        without_ranges = range_regex.sub(r"\2 ", pathseg)
        dt = _extract_datetime(without_ranges)
        if dt is None:
            dt = _extract_datetime(pathseg)
        if dt is not None:
            return dt
    return None


def infer_project_name(name: str) -> str:
    """Runs are filed by project; the longest folder name is the descriptive one."""
    folders = name.split("/")[:-1]
    return max(folders, key=len)


def _number(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if isnull(value):
        return None
    return value


def _sample_rows(df, sample_name):
    names = df.index.get_level_values("sample_name")
    if isnull(sample_name):
        # Rows that name a spot but no sample
        return df.loc[names.isnull()]
    return df.loc[names == sample_name]


class AgeCalcPipeline(Pipeline):
    """Cleaned ETAgeCalc and NuAgeCalc data tables."""

    name = "laserchron"
    file_type = "ETAgeCalc"
    pattern = "*.xls*"

    def read_source(self, source, path):
        # The name a file is known by carries its project and date
        return self.read_table(Path(path), PurePath(source.path).as_posix())

    def read(self, path):
        return self.read_table(Path(path), Path(path).as_posix())

    def read_table(self, path: Path, name: str):
        if "NUPM-MON" in PurePath(name).stem:
            raise DataTableError("NUPM-MON files are not handled yet")
        try:
            data, meta = normalize_data(read_datatable(path))
        except IndexError as err:
            raise DataTableError(str(err))
        data.index.name = "analysis"
        data = generalize_samples(data)

        project = Project(name=infer_project_name(name))
        # Dates are required; a run filed without one is given the earliest
        date = extract_datetime(name) or datetime.min

        for sample_name in data.index.unique(level=0):
            rows = _sample_rows(data, sample_name)
            yield self.session(rows, meta, project=project, date=date)

    def session(self, rows, meta, *, project, date) -> Session:
        sample_name = rows.index.unique(level="sample_name")[0]
        sample = None
        if not isnull(sample_name):
            sample = Sample(name=sample_name)

        # A spot listed twice is kept once
        rows = rows[~rows["analysis"].duplicated(keep="first")]
        return Session(
            date=date,
            project=project,
            sample=sample,
            analysis=[self.analysis(row, meta) for _, row in rows.iterrows()],
        )

    def analysis(self, row, meta) -> Analysis:
        try:
            index = int(row.name[2])
        except ValueError:
            index = None
        data = [self.datum(key, value, row, meta) for key, value in row.items()]
        return Analysis(
            session_index=index,
            analysis_name=str(row.name[1]),
            datum=[d for d in data if d is not None],
        )

    def datum(self, key, value, row, meta):
        if key == "analysis" or key.endswith("_error"):
            return None
        if key == "best_age":
            # One of the other ages is the best age; it is marked below
            return None
        value = _number(value)
        if value is None:
            return None

        error = None
        error_unit = None
        error_key = key + "_error"
        if error_key in row.index:
            error = _number(row.at[error_key])
            error_unit = meta[error_key].at["Unit"]

        is_age = key.startswith("age_")
        is_accepted = None
        if is_age:
            best_age = _number(row.at["best_age"])
            is_accepted = best_age is not None and isclose(
                value, best_age, rel_tol=1e-5, abs_tol=1e-8
            )
        return Datum(
            value=value,
            error=error,
            is_accepted=is_accepted,
            type=DatumType(
                parameter=meta[key].name,
                unit=meta[key].at["Unit"],
                error_unit=error_unit,
                error_metric="2s",
                is_interpreted=is_age,
            ),
        )
