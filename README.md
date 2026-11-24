# Sparrow at the Arizona LaserChron Center

The lab's own part of its [Sparrow](https://sparrow-data.org) installation: the
`laserchron` Python package, which holds its pipelines and schema extensions,
and the lab's configuration and site content.

## Pipelines

A pipeline turns files into Sparrow records and can be run anywhere, without a
Sparrow server.

- **`laserchron.pipeline:AgeCalcPipeline`** reads the `datatable` sheet of a
  cleaned ETAgeCalc or NuAgeCalc workbook. Each sample in a workbook becomes a
  session, each spot an analysis, and each column a datum; the age that matches
  the best age is marked as accepted. The project is taken from the longest
  folder name in the file's path and the date from the path as well, so files
  should be read from where they are filed.
- **`laserchron.metadata:SampleMetadataPipeline`** reads the lab's sample
  metadata sheet (`metadata/*.csv`): sample names, materials and locations.

```sh
pip install -e ".[dev]"   # with sparrow-cli and sparrow-pipelines from the Sparrow repository

# Read and validate, sending nothing anywhere
sparrow run laserchron.pipeline:AgeCalcPipeline "path/to/Project/Run 3-4 Aug 2014"

# Send the records to the lab
sparrow --env production run laserchron.pipeline:AgeCalcPipeline path/to/Project --load

pytest
```

On the lab's server the same pipelines are tasks, reading from the lab's
bucket: `sparrow run laserchron` and `sparrow run laserchron-metadata`.

## Server plugin

`laserchron.plugin` adds the pipelines and the schema in `laserchron/schema` to
a Sparrow server. It is found through the package's `sparrow.plugins` entry
point, so the lab's backend image is Sparrow's with this package installed
(`Dockerfile`).

## What is not handled

- Workbooks with no `datatable` sheet, `AGE PICK` files and `NUPM-MON` files are
  rejected, as before.
- Geologic entities in the metadata sheet are not recorded.
