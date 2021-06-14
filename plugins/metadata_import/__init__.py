from pandas.io import json
from sparrow.plugins import SparrowPlugin
from sparrow.context import app_context
from .cli import import_laserchron_metadata
from .utils import material_check

from pathlib import Path
import pandas as pd
import math

from IPython import embed

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

        insert_sample = here / 'insert-sample.sql' # db.exec_sql

        df = pd.read_csv(fn)

        df = df[df['Sample ID'].notna()] ## gets rid of any row that doesn't have a sample name

        # format long/lat columns
        df['Longitude'] = df['Longitude'].apply(self.clean_long_lat_to_float)
        df['Latitude'] = df['Latitude'].apply(self.clean_long_lat_to_float)

        # remove the rest of the unparseable coordinates
        df = self.drop_unparseable_coord(df)

        # create json list for the eventual load data
        json_list = self.create_sample_dict(df)
        # check if exsits
        json_list = self.check_if_exists(json_list)


            # load in all the rest of the data
        for ele in json_list:
            params={'name':ele['name'], 'material':ele['material'], 'location':ele['location']}
            try:
                db.exec_sql(insert_sample, params)
                print(f"Inserting {ele['name']}")
            except:
                embed()
                print('Something went very wrong')


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
                    setattr(existing, k, v)

                assert len(db.session.dirty) > 0
                
                try:
                    print("Added data to existing sample")
                    db.session.commit()
                except:
                    print("Error")
                    db.session.rollback()
        for i in reversed(samples_that_exist): # reverse so indexes stay in correct place
            del json_list[i]
        
        return json_list
    


    def create_sample_dict(self, df):
        """ Create sample dictionary ready for loading """
        """ ref_datum: null,
    ref_distance: null,
    ref_unit: null, """
        json_list = []

        for indx, row in df.iterrows():
            obj = {}
            obj['name'] = row['Sample ID']
            if str(row['Material'] == 'nan'):
                obj['material'] = None
            else:
                obj['material'] = row['Material']
                
            if row['Geologic Entity'] is not None:
                obj['sample_geo_entity'] = [{'ref_datum':None,'ref_distance':None,\
                    'ref_unit':None,'geo_entity':{'type': None, 'name': row['Geologic Entity']}}]
            if row['Longitude'] is None:
                obj['location'] = None
            obj['location'] = f"SRID=4326;POINT({row['Longitude']} {row['Latitude']})"
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
