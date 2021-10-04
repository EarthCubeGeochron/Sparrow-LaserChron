from sparrow.plugins import SparrowPlugin
from sparrow.context import app_context
from .cli import import_laserchron_metadata
from .utils import material_check

from pathlib import Path
import pandas as pd
import click
from IPython import embed

def space(spaces=1):
    for i in range(0, spaces):
        click.echo("")

def failed_import(name:str, e):
    click.secho(" Failed Import! ", fg="white", bg="red")
    click.secho(f"Sample {name} did not import correctly", fg="red")
    click.secho(f"Error: {e}", fg="yellow")

class LaserChronMetadataImporter(SparrowPlugin):

    name = "laserchron-metadata"
    minutes_tick = "′"
    degree_symbol = "°"

    def iterfiles(self, filename):
        """
        Read in csv and perform some data cleaning 
        """
        db = app_context().database

        here = Path(__file__).parent
        fn = here / filename

        click.secho(f"Reading data from {filename}", fg="blue")

        df = pd.read_csv(fn)

        df = df[df['Sample ID'].notna()]

        df['Longitude'] = df['Longitude'].apply(self.clean_long_lat_to_float)
        df['Latitude'] = df['Latitude'].apply(self.clean_long_lat_to_float)

        df = self.drop_unparseable_coord(df)

        json_list = self.create_sample_dict(df)
        json_list, number_existing = self.check_if_exists(json_list)

        total_samples= len(json_list)

        successfully_imported = 0

        for ele in json_list:
            try:
                db.load_data('sample', ele)
                click.secho(f"Inserting sample {ele['name']}", fg="green")
                successfully_imported += 1
            except Exception as e:
                failed_import(ele['name'], e)
        
        click.secho("Finished Importing Metadata", fg="bright_green")
        click.secho(f"{number_existing} samples already existed and checked for new metadata.", fg="bright_green")
        click.secho(f"{successfully_imported}/{total_samples} successfully imported!", fg="bright_green")


    def check_if_exists(self, json_list):
        """ 
        Check if sample exists in the database and if it does add the additional metadata
        """
        db = app_context().database
        Sample = db.model.sample

        samples_that_exist = []
        for inx, row in enumerate(json_list):
            name = row['name']
            res = db.session.query(Sample).filter_by(name=name).all()
            material_check(db, row['material'])
            if len(res) > 0:
                ## sample already exists
                samples_that_exist.append(inx)
                existing = res[0]
                for k,v in row.items():
                    if k == "location":
                        #'SRID=4269;POINT(-71.064544 42.28787)'
                        v = f"SRID=4269;POINT({v['coordinates'][0]} {v['coordinates'][1]})"
                    setattr(existing, k, v)

                assert len(db.session.dirty) > 0
                
                try:
                    click.secho(f"{name} already exists, adding missing metadata..", fg="yellow")
                    db.session.commit()
                except Exception as e:
                    failed_import(name, e)
                    db.session.rollback()
        for i in reversed(samples_that_exist): # reverse so indexes stay in correct place
            del json_list[i]
        
        return json_list, len(samples_that_exist)

    def create_sample_dict(self, df):
        """ Create sample dictionary ready for loading """
        """ ref_datum: null,
            ref_distance: null,
            ref_unit: null, """
        json_list = []

        for indx, row in df.iterrows():
            obj = {}
            obj['name'] = row['Sample ID']
            if str(row['Material']) == 'nan':
                obj['material'] = None
            else:
                obj['material'] = row['Material']
            if str(row['Geologic Entity']) is not None:
                geo_entity = {'name': str(row['Geologic Entity'])}
                if str(row["Geologic Period"]) is not None:
                   geo_entity['description'] = str(row['Geologic Period'])
                obj['sample_geo_entity'] = [{'geo_entity':geo_entity}]
            obj['location'] = None
            if row['Longitude'] is not None:
                obj['location'] = {"coordinates": [row['Longitude'], row['Latitude']], "type":"Point"}
            json_list.append(obj)

        return json_list
    
    def parse_researchers(self, df):
        """ Parse researcher from Analyst Name column

            Case: more than one in cell separated by a '/'
        """
    
    def clean_long_lat_to_float(self, coordinate):
        """ 
        Checks for a few cases where the long/lat value isn't float-parseable 

        returns coordinate as decimal float
        """

        coordinate_str = str(coordinate)

        if coordinate_str == 'nan':
            return None

        unformatted_str, is_neg = self.sign_check(coordinate_str)
        if self.degree_symbol in coordinate_str:
            if self.minutes_tick in unformatted_str:
                # parse out 87°30′00′′ --> 87.5
                coordinate_float = self.clean_non_decimal_string(unformatted_str)
            else:
                # remove degree symbol which is MOST likely at the end
                unformatted_str = unformatted_str[:-1] 
                coordinate_float = float(unformatted_str)
        else:
            try:
                coordinate_float = float(unformatted_str)
            except:
                return coordinate
        if is_neg and coordinate_float > 0:
            coordinate_float = coordinate_float * -1
        return coordinate_float
    
    def to_decimal(self, degrees, minutes, seconds):
        """ turns degrees° min′ sec′′ to decimal style coordinate """
        return degrees + minutes/60 + seconds/3600

    def clean_non_decimal_string(self, unformatted_str):
        """ function to turn degree to a decimal formatted coordinate """

        degrees, min_sec, *rest =  tuple(unformatted_str.split(self.degree_symbol))
        minutes, seconds, *rest = tuple(min_sec.split(self.minutes_tick))

        return self.to_decimal(int(degrees), int(minutes), int(seconds))

    def sign_check(self, unformatted_str):
        """ 
        Takes an unformatted coordinate string and checks if one of the cardinal directions is in it.cardinal_direction_to_sign

        Removes the letter from the unformatted string and returns it and a bool indicated whether it's negative or not.

        Will also check for a minus sign

        returns (unformatted_string, boolean)
        """

        letters= ['N','S','E','W']
        neg = ['S','W']

        is_neg = False

        if unformatted_str[0] == '-':
            is_neg = True

        for l in letters:
            if l in unformatted_str:
                unformatted_str = unformatted_str.replace(l, '')
                if l in neg:
                    is_neg = True

        return unformatted_str, is_neg
    
    def drop_unparseable_coord(self, df):
        """ 
            Loop through coordinate columns and 
            remove those that aren't float parseable 
        """
        unparseable_indexes = []
        for index,row in df.iterrows():
            try:
                float(row['Longitude'])
            except:
                unparseable_indexes.append(index)
        
        df.drop(index=unparseable_indexes, inplace=True)

        return df
    
    def on_setup_cli(self, cli):
        cli.add_command(import_laserchron_metadata)
