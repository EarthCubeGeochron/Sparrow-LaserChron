from math import isnan
from click import secho, echo, style
from pandas import concat, to_numeric
from click import secho
import re

from sparrow.import_helpers import SparrowImportError


def extract_analysis_name(sample_name):
    sep = r"[\.\:_\s-]"

    # We have something in the form "Spot xxx"
    pat = sep + r"(Spot.+)$"
    s = re.search(pat, str(sample_name), flags=re.IGNORECASE)
    if s is not None:
        return s.group(1)

    # We just have a delimited last word (without a spot)
    pat = sep + r"(\w+)$"
    try:
        s = re.search(pat, str(sample_name), flags=re.IGNORECASE)
    except TypeError:
        return None
    if s is not None:
        return s.group(1)

    pat = sep + r"(\d+)$"
    s = re.search(pat, str(sample_name))
    if s is not None:
        return s.group(1)
    return None


def remove_suffix(s1, s2):
    try:
        if s1.endswith(s2):
            return s1[: -len(s2)]
    except (TypeError, AttributeError):
        pass
    return s1


delimiters = ".:_- "


def strip_analysis_name(row):
    s1 = row["analysis"]
    s2 = row["analysis_name"]
    return str(remove_suffix(s1, s2)).rstrip(delimiters)


def print_sample_info(df, verbose=False):
    if verbose:
        echo(style("Samples: ", bold=True), err=True)
        for id, group in df.groupby(["sample_name"]):
            n = style(f" ({len(group)})", dim=True)
            echo("- " + style(id, fg="cyan") + n, err=True)
        echo("", err=True)
    else:
        echo(
            style("Samples: ", bold=True) + "   ".join(data.sample_id.unique()),
            err=True,
        )


def generalize_samples(input):
    """Generalize sample ids into `sample_name`, `analysis_name`,
    and `session_index` columns"""

    # Create sample name columns
    data = input.reset_index()
    data.rename(columns={"Analysis": "analysis"}, inplace=True)

    # Strip out extra data
    data["analysis"] = data["analysis"].str.strip(delimiters)
    data["analysis_name"] = data["analysis"].apply(extract_analysis_name)
    # Strip the analysis suffix off of the sample ID
    data["sample_name"] = data.apply(strip_analysis_name, axis=1)

    for sample_name, group in data.groupby(["sample_name"]):
        unique_suffix = group["analysis_name"].unique()
        # If we don't have enough unique suffixes, it's probable that we actually
        # grabbed part of the sample ID. In that case, we fall back to the
        # original sample id
        ix = data["sample_name"] == sample_name
        if len(unique_suffix) / len(group) < 0.4:
            # Not many of our analysis names are unique, so we fall back
            # to dealing with samples without internal enumeration
            data.loc[ix, "sample_name"] = data.loc[ix, "analysis"]
            data.loc[ix, "analysis_name"] = None

        if sample_name.startswith("Spot"):
            # It appears we don't have a sample name, instead
            data.loc[ix, "sample_name"] = None
            data.loc[ix, "analysis_name"] = data.loc[ix, "analysis"]

    n_samples = len(data["sample_name"].unique())
    if n_samples > 0.3 * len(data) and n_samples > 20:
        # We have a lot of "unique" samples even though we tried to extract
        # sample IDs. We are probably doing something wrong.
        raise SparrowImportError("Too many unique samples; skipping import.")

    # Session index is extracted if an integer can be found easily
    cleaned_name = data["analysis_name"].str.replace("Spot", "").str.strip(delimiters)
    data["session_index"] = to_numeric(
        cleaned_name, errors="coerce", downcast="integer"
    )

    print_sample_info(data, verbose=True)

    return data.set_index(["sample_name", "analysis_name", "session_index"], drop=True)


def list_sample_names(data_files, verbose=False):
    """List sample names found in a set of CSV data tables, for debugging purposes."""

    from .laserchron_importer import decode_datatable

    for i, file in enumerate(data_files):
        print(file.file_path)
        try:
            df, meta = decode_datatable(file.csv_data)
            if verbose:
                for line in wrap("  ".join([f"{i:20}" for i in df.index]), 80):
                    print(line)
            df1 = generalize_samples(df)

        except SparrowImportError as err:
            secho(str(err), fg="red")
        print("")
