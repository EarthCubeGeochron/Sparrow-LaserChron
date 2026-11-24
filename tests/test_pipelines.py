from datetime import datetime
from pathlib import Path
from shutil import copy

from pytest import fixture, raises
from sparrow.pipelines import DryRunSink, run_pipeline

from laserchron.metadata import SampleMetadataPipeline, parse_coordinate
from laserchron.pipeline import AgeCalcPipeline, extract_datetime, infer_project_name
from laserchron.sample_names import DataTableError

root = Path(__file__).parent.parent
data = root / "tests" / "data"


@fixture(scope="module")
def run_dir(tmp_path_factory):
    """Workbooks filed the way the lab files them: project, then run."""
    root = tmp_path_factory.mktemp("lab")
    run = root / "Smith detrital zircons" / "Run 3-4 Aug 2014"
    run.mkdir(parents=True)
    copy(data / "F-90_E2AgeCalc_DataTable.xlsx", run / "F-90.xlsx")
    copy(data / "Test_E2_Table.xls", run / "no-datatable.xls")
    return root


@fixture(scope="module")
def result(run_dir):
    sink = DryRunSink()
    report = run_pipeline(AgeCalcPipeline(), [run_dir], sink)
    return report, sink


def test_files(result):
    report, sink = result
    assert report.count("loaded") == 1
    # A workbook without a `datatable` sheet is rejected, not half-read
    assert report.count("failed") == 1
    assert "No data table" in list(sink.failures.values())[0]


def test_session(result):
    _, sink = result
    (session,) = sink.records
    assert session.sample.name == "Quinn F-90 16June2017"
    assert session.project.name == "Smith detrital zircons"
    assert session.date == datetime(2014, 8, 4)
    assert len(session.analysis) == 305


def test_analysis(result):
    _, sink = result
    analysis = sink.records[0].analysis[0]
    assert analysis.analysis_name.startswith("Spot")
    assert analysis.session_index is not None

    by_parameter = {d.type.parameter: d for d in analysis.datum}
    assert "best_age" not in by_parameter
    assert not any(p.endswith("_error") for p in by_parameter)

    age = by_parameter["age_206Pb_238U"]
    assert age.type.unit == "Ma"
    assert age.type.error_unit == "Ma"
    assert age.type.error_metric == "2s"
    assert age.type.is_interpreted
    assert age.error is not None

    uranium = by_parameter["U"]
    assert uranium.type.unit == "ppm"
    assert not uranium.type.is_interpreted
    assert uranium.is_accepted is None


def test_one_best_age_per_analysis(result):
    _, sink = result
    for analysis in sink.records[0].analysis:
        accepted = [d for d in analysis.datum if d.is_accepted]
        assert len(accepted) == 1
        assert accepted[0].type.parameter.startswith("age_")


def test_records_are_importable(result):
    _, sink = result
    record = sink.records[0].to_import_data()
    assert record["sample"] == {"name": "Quinn F-90 16June2017"}
    assert record["analysis"][0]["datum"][0]["type"]["unit"]


def test_names():
    name = "Smith 2014/Run 3-4 Aug 2014/x.xls"
    assert infer_project_name(name) == "Run 3-4 Aug 2014"
    assert extract_datetime(name) == datetime(2014, 8, 4)
    assert extract_datetime("nodate/file.xls") is None


def test_unhandled_files(tmp_path):
    path = tmp_path / "Project" / "NUPM-MON run.xlsx"
    path.parent.mkdir()
    copy(data / "F-90_E2AgeCalc_DataTable.xlsx", path)
    with raises(DataTableError):
        list(AgeCalcPipeline().read(path))


def test_coordinates():
    assert parse_coordinate("87°30′00′′") == 87.5
    assert parse_coordinate("87.5°") == 87.5
    assert parse_coordinate("87.5 W") == -87.5
    assert parse_coordinate("-125.429") == -125.429
    assert parse_coordinate(float("nan")) is None
    assert parse_coordinate("unknown") is None


def test_sample_metadata():
    samples = list(
        SampleMetadataPipeline().read(root / "metadata" / "alc_metadata.csv")
    )
    assert len(samples) > 10
    assert all(s.location is not None for s in samples)

    # The sheet lists some samples twice; each is recorded once, as last entered
    names = [s.name for s in samples]
    assert len(names) == len(set(names))
    sample = next(s for s in samples if s.name == "14IY01")
    assert sample.material == "zircon"
    assert sample.location.coordinates == (-125.42921, 50.714433)
