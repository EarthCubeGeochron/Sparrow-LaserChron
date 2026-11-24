"""Sample metadata, from the spreadsheet the lab keeps: where a sample is from
and what it is made of."""

from pandas import isnull, read_csv
from sparrow.pipelines import Pipeline, Point, Sample

MINUTES = "′"
DEGREES = "°"


def _sign_check(text):
    """Strip a cardinal direction, noting whether it points south or west."""
    is_negative = text.startswith("-")
    for letter in "NSEW":
        if letter in text:
            text = text.replace(letter, "")
            if letter in "SW":
                is_negative = True
    return text, is_negative


def parse_coordinate(value):
    """A coordinate as decimal degrees, or None if it cannot be read.
    Handles `87°30′00′′`, `87.5°`, `87.5 W` and plain numbers."""
    text = str(value)
    if text == "nan":
        return None
    text, is_negative = _sign_check(text)
    try:
        if DEGREES in text and MINUTES in text:
            degrees, rest, *_ = text.split(DEGREES)
            minutes, seconds, *_ = rest.split(MINUTES)
            number = int(degrees) + int(minutes) / 60 + int(seconds) / 3600
        elif DEGREES in text:
            number = float(text[:-1])
        else:
            number = float(text)
    except ValueError:
        return None
    if is_negative and number > 0:
        number = -number
    return number


class SampleMetadataPipeline(Pipeline):
    name = "laserchron-metadata"
    file_type = "Sample metadata"
    pattern = "*.csv"

    def read(self, path):
        df = read_csv(path)
        df = df[df["Sample ID"].notna()]
        # The sheet lists some samples more than once; the later entry stands
        df = df.drop_duplicates("Sample ID", keep="last")
        for _, row in df.iterrows():
            longitude = parse_coordinate(row["Longitude"])
            latitude = parse_coordinate(row["Latitude"])
            if longitude is None or latitude is None:
                # Only samples with a readable location are recorded from here
                continue
            location = Point(coordinates=(longitude, latitude))
            material = None if isnull(row["Material"]) else str(row["Material"])
            yield Sample(
                name=str(row["Sample ID"]), material=material, location=location
            )
