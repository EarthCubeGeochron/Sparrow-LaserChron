"""What the lab adds to a Sparrow server: its schema, and its pipelines as tasks."""

from pathlib import Path

from macrostrat.schema_management import SchemaDefinition
from sparrow.core.plugins import SparrowPlugin


class LaserChronPlugin(SparrowPlugin):
    name = "laserchron"

    def on_define_schema(self, chunks):
        chunks.append(
            SchemaDefinition(
                name="laserchron",
                depends_on=["core"],
                provides=[Path(__file__).parent / "schema"],
            )
        )

    def on_define_pipelines(self, pipelines):
        from .metadata import SampleMetadataPipeline
        from .pipeline import AgeCalcPipeline

        pipelines.extend([AgeCalcPipeline(), SampleMetadataPipeline()])
