from math import isnan
from click import secho, echo, style
from pandas import concat, to_numeric
from click import secho
import re

from sparrow.import_helpers import SparrowImportError

def extract_analysis_suffix(sample_name):
    sep = r"[\.\:_\s-]"
    pat = sep+r"(Spot )?(\w+)$"
    try:
        s = re.search(pat, str(sample_name), flags=re.IGNORECASE)
    except TypeError:
        return None
    if s is not None:
        return s.group(2)

    pat = sep+r"(\d+)$"
    s = re.search(pat, str(sample_name))
    if s is not None:
        return s.group(1)
    return None


def remove_suffix(s1, s2):
    if s1.endswith(s2):
        return s1[:-len(s2)]
    return s1


def strip_analysis_suffix(row):
    s1 = str(row['analysis'])
    s2 = str(row['analysis_suffix'])
    return (remove_suffix(s1, str(s2))
            .rstrip('.:_- '))

def generalize_samples(data):
    """Generalize sample ids into `sample_name`, `analysis_name`,
       and `session_index` columns"""

    # Create sample name columns
    data.reset_index(inplace=True)
    data.rename(columns={'Analysis': 'analysis'}, inplace=True)

    # Strip out extra data
    data['analysis'] = data['analysis'].str.strip()
    data['analysis_suffix'] = data['analysis'].apply(extract_analysis_suffix)
    data['sample_id'] = data.apply(strip_analysis_suffix, axis=1)

    print(data['sample_id'].unique())

    for ix, group in data.groupby(["sample_id"]):
        unique_suffix = group['analysis_suffix'].unique()
        # If we don't have enough unique suffixes, we
        # fall back to the original sample id
        _ix = data['sample_id'] == ix
        if len(unique_suffix)/len(group) < 0.8:
            data.loc[_ix, 'sample_id'] = data.loc[_ix, 'analysis']

    print(data)


    # Session index should be integer, so we set things that don't match
    # to NaNs
    data['session_ix'] = to_numeric(
        data['analysis_suffix'],
        errors='coerce',
        downcast='integer')
    data.drop('analysis_suffix', axis=1, inplace=True)

    echo(style("Samples: ", bold=True)+"   ".join(data.sample_id.unique()))

    return data.set_index(["sample_id", "session_ix"], drop=True)

def list_sample_names(data_files, verbose=False):
    from .laserchron_importer import decode_datatable

    """List sample names found in a set of CSV data tables, for debugging purposes."""
    for i, file in enumerate(data_files):
        print(file.file_path)
        try:
            df, meta = decode_datatable(file.csv_data)
            if verbose:
                for line in wrap("  ".join([f"{i:20}" for i in df.index]), 80):
                    print(line)
            df1 = generalize_samples(df)
            print(df1.index)
            ##secho("  ".join(df1.index.unique()), fg='cyan')

        except SparrowImportError:
            secho("Encountered an error!", fg='red')
        print("")
