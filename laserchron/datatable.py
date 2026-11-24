"""Reading and tidying the `datatable` sheet of an AgeCalc workbook."""

from io import StringIO
from pathlib import Path

from pandas import DataFrame, concat, isna, read_csv, read_excel

from .sample_names import DataTableError


def read_datatable(path: Path) -> DataFrame:
    """The `datatable` sheet as it was written, without interpretation."""
    try:
        df = read_excel(path, sheet_name="datatable", header=None)
    except ValueError as err:
        if "AGE PICK" in Path(path).stem:
            raise NotImplementedError("AGE PICK files are not yet handled")
        raise DataTableError(f"No data table ({err})")
    # Through CSV and back: the first row becomes the header and every cell is
    # parsed from text, which is the form the tidying below was written for
    text = StringIO()
    df.to_csv(text)
    text.seek(0)
    return read_csv(text).iloc[:, 1:]


def merge_cols(d):
    v1 = d.iloc[0]
    v2 = d.iloc[1]

    if isna(v2):
        return v1

    v = (v1, v2)
    if "Pb" in v1 or "Th" in v2:
        return "/".join(v)
    return " ".join(v)


def table_metadata(headers):
    """Column descriptions and units, from the two header rows."""
    columns = headers.apply(merge_cols, axis=0)
    units = columns.str.extract(r"\((.+)\)").iloc[:, 0]

    # Extract units and make sure all are defined
    for i, u in enumerate(units):
        if str(u) != "nan":
            continue
        try:
            next_col_unit = units.iat[i + 1]
        except IndexError:
            next_col_unit = None
        c = columns.iat[i]
        if next_col_unit == "Ma":
            # Set this unit to Ma...
            units.iat[i] = "Ma"
            columns.iat[i] = str(c) + " age"
        elif "/" in c and "age" not in c:
            units.iat[i] = "ratio"
        elif "error corr" in c:
            units.iat[i] = "dimensionless"

    # Get rid of units
    columns = columns.str.strip().str.replace(r" \(.+\)$", "", regex=True)

    for i, col in enumerate(columns):
        if col.strip() != "±":
            continue
        columns.iat[i] = columns.iat[i - 1] + " error"

    # Clean columns
    ix = (
        columns.str.replace(r"[\*\/\s\.]+", " ", regex=True)
        .str.strip()
        .str.replace(r"^(\d{3}\w{1,2}\s\d{3}\w{1,2}) age", r"age \1", regex=True)
        .str.replace("Best age", "best age")
        .str.replace("^Conc$", "concordance", regex=True)
        .str.replace(r"\s+", "_", regex=True)
    )

    meta = concat((columns, units), axis=1).transpose().rename(columns=ix)
    meta.columns.name = "Column"
    meta.index = ["Description", "Unit"]
    meta.loc["Description", "U"] = "Uranium concentration"
    meta.loc["Description", "concordance"] = "Concordance"

    return meta


def normalize_data(df):
    """Split a data table into its measurements and what each column means."""
    if df.iloc[0, 0].startswith("Table"):
        df = df[1:]

    # Drop empty rows
    df = df.dropna(how="all")

    # Drop columns that are empty in the header
    # Note: this does not preserve comments and some other metadata;
    # we may want to.
    header = df.iloc[1:3, 1:].copy()
    # Make sure second row of header (chiefly units) is set to null
    # if first row is null (this helps get rid of trailing end matter)
    header.iloc[1] = header.iloc[1].mask(header.iloc[0].isnull())
    header = header.dropna(axis=1, how="all")
    meta = table_metadata(header)

    body = df.iloc[3:].set_index(df.columns[0])
    body.index.name = "Analysis"
    # Make sure data is the same shape as headers
    data = body.drop(columns=body.columns.difference(header.columns)).dropna(how="all")

    # We've found a few empty data frames
    if data.empty:
        raise DataTableError("Empty dataframe")

    if data.shape[1] != meta.shape[1]:
        raise DataTableError("Data frame is not correct shape")

    # For some reason we have a lot of these closed brackets in data files
    data.index = data.index.str.replace(" <>", "").str.strip()
    data.columns = meta.columns

    ncols = 19
    if data.shape[1] == ncols:
        return data, meta

    if len(data.columns[:ncols].intersection(data.columns[ncols:])) > 0:
        # Ignore duplicate output columns
        data = data.iloc[:, :ncols]
        meta = meta.iloc[:, :ncols]

    return data, meta
