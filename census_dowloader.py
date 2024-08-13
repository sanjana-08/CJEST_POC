import csv
import json
import subprocess
import zipfile
import shutil
import uuid
from pathlib import Path
import requests
import geopandas as gpd
import os
import urllib3
import sys
import pandas as pd
import censusdata
from typing import List


# Constants
#BASE_URL_FIPS = "https://www.census.gov/geographies/reference-files/time-series/geo/tallies.html"
#BASE_URL_FIPS = "https://www2.census.gov/geo/docs/maps-data/data/geo_tallies/census_block_tally.txt"
BASE_URL_FIPS="https://www2.census.gov/geo/docs/maps-data/data/geo_tallies2020/2020talliesbystate.xlsx"
ACS_URL =""
DECENNIAL_URL = ""
CENSUS_ACS_FIPS_CODES_TO_SKIP = ["60", "66", "69", "78"]
class GeoFileType:
    SHP = 1
    GEOJSON = 2
    CSV = 3
    TXT = 4

class Downloader:
    """A class to handle downloading of files from URLs."""

    @classmethod
    def download_file_from_url(cls, file_url: str, download_file_name: Path, verify: bool = True) -> str:
        """Downloads a file from a URL and returns the file location."""
        print(f"Downloading file from URL: {file_url}")
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        if not download_file_name.parent.exists():
            print(f"Creating directory: {download_file_name.parent}")
            download_file_name.parent.mkdir(parents=True, exist_ok=True)

        response = requests.get(file_url, verify=verify, timeout=10)
        if response.status_code == 200:
            with open(download_file_name, "wb") as file:
                file.write(response.content)
            print(f"File downloaded successfully: {download_file_name}")
        else:
            sys.exit(f"HTTP response {response.status_code} from URL {file_url}. Info: {response.content}")
        print("file now in", download_file_name)
        
        return download_file_name

    @classmethod
    def download_zip_file_from_url(cls, file_url: str, unzipped_file_path: Path, verify: bool = True) -> None:
        """Downloads and extracts a zip file from a URL."""
        print(f"Downloading and extracting ZIP file from URL: {file_url}")
        dir_id = uuid.uuid4()
        zip_download_path = Path("data/tmp/downloads") / f"{dir_id}" / "download.zip"

        zip_file_path = cls.download_file_from_url(file_url=file_url, download_file_name=zip_download_path, verify=verify)

        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(unzipped_file_path)
            print("///////////////")
        print(zip_download_path.parent)
        # cleanup temporary file and directory
        shutil.rmtree(zip_download_path.parent,ignore_errors=True)
        
        


class CensusETL:
    """Class to perform ETL (Extract, Transform, Load) operations for census data."""
    GEOJSON_BASE_PATH = Path("data/census/geojson")
    CSV_BASE_PATH = Path("data/census/csv")
    #SHP_BASE_PATH = Path("data/census/shp")  # Add SHP base path
    #shape_file_path = Path("data/census/shp")
    NATIONAL_TRACT_CSV_PATH = CSV_BASE_PATH / "us.csv"
    NATIONAL_TRACT_JSON_PATH = GEOJSON_BASE_PATH / "us.json"
    GEOID_TRACT_FIELD_NAME: str = "GEOID10_TRACT"
    
    CENSUS_ACS_PATH = Path("data/census/pop")
    CENSUS_DECENNIAL_PATH = Path("data/census/pop")

  

    def __init__(self):
        self.shape_file_path = Path("data/census/shp")
        self.STATE_FIPS_CODES = []
        self.TRACT_PER_STATE = {}  # in-memory dict per state
        self.TRACT_NATIONAL = []  # in-memory global list
    
    def fetch_and_extract_fips_codes(self, data_path: Path):
        """Fetches and extracts FIPS codes."""
        print(f"Fetching FIPS codes from URL: {BASE_URL_FIPS}")
        
        fips_xlsx_path = data_path / "census" / "csv" /"census_block_tally.xlsx"
        
        fips_xlsx_path.parent.mkdir(parents=True, exist_ok=True)
        Downloader.download_file_from_url(BASE_URL_FIPS, fips_xlsx_path)
        
        # Check if the file exists and log
        if not fips_xlsx_path.is_file():
            sys.exit(f"FIPS CSV file not found at path {fips_xlsx_path}")
        print(f"Reading FIPS codes from {fips_xlsx_path}")

             
        df = pd.read_excel(fips_xlsx_path, engine='openpyxl',dtype=str)

        
        statefp_column = df['STATEFP'].astype(str).tolist()
       

      
        for fips_code in statefp_column:
            if fips_code!="nan":
                self.STATE_FIPS_CODES.append((fips_code))
                #print(f"Fetched FIPS code: {fips_code}")

        

    def _path_for_fips_file(self, fips_code: str, file_type: GeoFileType) -> Path:
        """Returns the file path for a given FIPS code and file type."""
        if file_type == GeoFileType.SHP:
            return self.shape_file_path / fips_code / f"tl_2010_{fips_code}_tract10.shp"
        elif file_type == GeoFileType.GEOJSON:
            return self.GEOJSON_BASE_PATH / f"{fips_code}.json"
        elif file_type == GeoFileType.CSV:
            return self.CSV_BASE_PATH / f"{fips_code}.csv"
        elif file_type == GeoFileType.TXT:
            return self.CSV_BASE_PATH / f"{fips_code}.txt"

        return Path()


    def get_data_sources(self):
        """Returns a list of URLs and destinations for state shapefiles."""
        print("Generating data sources for shapefiles.")
        sources = []
        for fips_code in self.STATE_FIPS_CODES:
            print("inside datasources")
            print(type(fips_code))
            tract_state_url = f"https://www2.census.gov/geo/tiger/TIGER2010/TRACT/2010/tl_2010_{fips_code}_tract10.zip"
            destination_path = self.shape_file_path / fips_code
            sources.append((tract_state_url, destination_path))
            print(f"Source: {tract_state_url}, Destination: {destination_path}")
         
        return sources
    
    def _fips_from_censusdata_censusgeo(self,censusgeo: censusdata.censusgeo) -> str:
        """Create a FIPS code from the proprietary censusgeo index."""
        #print("inside censusgeo")
        #print(censusgeo.params())
        fips = "".join([value for (key, value) in censusgeo.params()])
        #print("fips", fips)
        return fips
    
    def get_population_data(self, data_path:Path):
        """Downloads census acs dataset that hold population data"""
        
        variables =[ "B01001_003E",  # Estimate!!Total:!!Male:!!Under 5 years
            "B01001_004E",  # Estimate!!Total:!!Male:!!5 to 9 years
            "B01001_005E",  # Estimate!!Total:!!Male:!!10 to 14 years
            "B01001_006E",  # Estimate!!Total:!!Male:!!15 to 17 years
            "B01001_007E",  # Estimate!!Total:!!Male:!!18 and 19 years
            "B01001_008E",  # Estimate!!Total:!!Male:!!20 years
            "B01001_009E",  # Estimate!!Total:!!Male:!!21 years
            "B01001_010E",  # Estimate!!Total:!!Male:!!22 to 24 years
            "B01001_011E",  # Estimate!!Total:!!Male:!!25 to 29 years
            "B01001_012E",  # Estimate!!Total:!!Male:!!30 to 34 years
            "B01001_013E",  # Estimate!!Total:!!Male:!!35 to 39 years
            "B01001_014E",  # Estimate!!Total:!!Male:!!40 to 44 years
            "B01001_015E",  # Estimate!!Total:!!Male:!!45 to 49 years
            "B01001_016E",  # Estimate!!Total:!!Male:!!50 to 54 years
            "B01001_017E",  # Estimate!!Total:!!Male:!!55 to 59 years
            "B01001_018E",  # Estimate!!Total:!!Male:!!60 and 61 years
            "B01001_019E",  # Estimate!!Total:!!Male:!!62 to 64 years
            "B01001_020E",  # Estimate!!Total:!!Male:!!65 and 66 years
            "B01001_021E",  # Estimate!!Total:!!Male:!!67 to 69 years
            "B01001_022E",  # Estimate!!Total:!!Male:!!70 to 74 years
            "B01001_023E",  # Estimate!!Total:!!Male:!!75 to 79 years
            "B01001_024E",  # Estimate!!Total:!!Male:!!80 to 84 years
            "B01001_025E",  # Estimate!!Total:!!Male:!!85 years and over
            "B01001_027E",  # Estimate!!Total:!!Female:!!Under 5 years
            "B01001_028E",  # Estimate!!Total:!!Female:!!5 to 9 years
            "B01001_029E",  # Estimate!!Total:!!Female:!!10 to 14 years
            "B01001_030E",  # Estimate!!Total:!!Female:!!15 to 17 years
            "B01001_031E",  # Estimate!!Total:!!Female:!!18 and 19 years
            "B01001_032E",  # Estimate!!Total:!!Female:!!20 years
            "B01001_033E",  # Estimate!!Total:!!Female:!!21 years
            "B01001_034E",  # Estimate!!Total:!!Female:!!22 to 24 years
            "B01001_035E",  # Estimate!!Total:!!Female:!!25 to 29 years
            "B01001_036E",  # Estimate!!Total:!!Female:!!30 to 34 years
            "B01001_037E",  # Estimate!!Total:!!Female:!!35 to 39 years
            "B01001_038E",  # Estimate!!Total:!!Female:!!40 to 44 years
            "B01001_039E",  # Estimate!!Total:!!Female:!!45 to 49 years
            "B01001_040E",  # Estimate!!Total:!!Female:!!50 to 54 years
            "B01001_041E",  # Estimate!!Total:!!Female:!!55 to 59 years
            "B01001_042E",  # Estimate!!Total:!!Female:!!60 and 61 years
            "B01001_043E",  # Estimate!!Total:!!Female:!!62 to 64 years
            "B01001_044E",  # Estimate!!Total:!!Female:!!65 and 66 years
            "B01001_045E",  # Estimate!!Total:!!Female:!!67 to 69 years
            "B01001_046E",  # Estimate!!Total:!!Female:!!70 to 74 years
            "B01001_047E",  # Estimate!!Total:!!Female:!!75 to 79 years
            "B01001_048E",  # Estimate!!Total:!!Female:!!80 to 84 years
            "B01001_049E",  # Estimate!!Total:!!Female:!!85 years and over,
       
        ]
       
      
        tract_output_field_name=self.GEOID_TRACT_FIELD_NAME
        


        print(f"Fetching ACS CENSUS data from URL: {ACS_URL}")
        acs_data_path = data_path / "census" / "pop" /"acs_census"/"acs_census_data.csv"
        print(self.STATE_FIPS_CODES)
        
        self.TOTAL_POPULATION_FROM_AGE_TABLE = "B01001_001E"
        self.STATE_GEOID_FIELD_NAME = "GEOID10"
        variable = [self.TOTAL_POPULATION_FROM_AGE_TABLE]

        for fips in self.STATE_FIPS_CODES:
            dfs = []
           
            if fips in CENSUS_ACS_FIPS_CODES_TO_SKIP:
                print(
                f"Skipping download for state/territory with FIPS code {fips}"
            )
            else:
                census_api_key = "" #already added to env
                if os.environ.get("CENSUS_API_KEY"):
                    census_api_key = "with API key"
                print(
                    f"Downloading data for state/territory with FIPS code {fips} {census_api_key}"
                )
                try:
                   
                    response = censusdata.download(
                        "acs5",
                        2010,
                        censusdata.censusgeo([("state", fips), ("county", "*"), ("tract", "*")]),
                        var=variable,
                        key =os.environ.get("CENSUS_API_KEY"),
                    )
                    dfs.append(response)

                except ValueError as e:
                    print(
                        f"Could not download data for state/territory with FIPS code {fips} because {e}"
                    )
                    raise e
           
                dfs = pd.DataFrame(dfs[0])
          
                dfs[tract_output_field_name] = dfs.index.to_series().apply(
            func=self._fips_from_censusdata_censusgeo
        )
                acs_data_path.parent.mkdir(parents=True, exist_ok=True)
                acs_data_path_st = data_path / "census" / "pop" /"acs_census"/f"acs_census_data_state_{fips}.csv"
                dfs.to_csv(acs_data_path_st, index=False) 
        
        
  

    def merge_csv_shp_file(self,data_path):
     
       
       for fips in self.STATE_FIPS_CODES:
            if fips in CENSUS_ACS_FIPS_CODES_TO_SKIP:
                print(
                f"Skipping download for state/territory with FIPS code {fips}"
            )
            else:

                shapefile_path = data_path / "census" / "shp" /f"{fips}"/f"tl_2010_{fips}_tract10.shp"
                csv_path =  data_path / "census" / "pop" /"acs_census"/f"acs_census_data_state_{fips}.csv"
                try:
                    gdf_shapefile = gpd.read_file(shapefile_path)
                    
                    df_csv = pd.read_csv(csv_path)
                    #df_csv["fips"] = df_csv["fips"].astype(str).apply(lambda x: x.zfill(2))
                    df_csv.rename(columns={'B01001_001E': 'TOTAL POPULATION ESTIMATE'}, inplace=True)
                    
 
            

                    # Ensure that the merge columns have the same data type
                    gdf_shapefile['GEOID10'] = gdf_shapefile['GEOID10'].astype(int)
                    df_csv['GEOID10_TRACT'] = df_csv['GEOID10_TRACT'].astype(int)
                    # Merge on the common key
            # Replace 'common_key' with the actual column name used for merging
                    merged_gdf = gdf_shapefile.merge(df_csv, how='left', left_on='GEOID10', right_on= 'GEOID10_TRACT')
                    # Save the updated GeoDataFrame to a new shapefile
                    updated_shapefile_path = data_path / "census" / "merged" /f"tl_2010_{fips}_tract10_merged"
                    updated_shapefile_path.mkdir(parents=True, exist_ok=True)
                    merged_gdf.to_file(updated_shapefile_path, driver='ESRI Shapefile')
                except ValueError as e:
                        print(
                            f"Could not find data for state/territory with FIPS code {fips} because {e}"
                        )
                        raise e

         
            





if __name__ == "__main__":
    data_path = Path("data")
    downloader = Downloader()
    census_etl = CensusETL()

    # Step 1: Fetch and extract FIPS codes
    print("Starting FIPS codes extraction.")
    census_etl.fetch_and_extract_fips_codes(data_path)

    # #Step 2: Download zip files and extract them
    # print("Downloading and extracting shapefiles.")
    # for url, destination in census_etl.get_data_sources():
    #     print("url", url)
    #     #print("Downloading from", url, "to",destination)
    #     print("Destination", destination)
    #     downloader.download_zip_file_from_url(url, destination)

    # print("Downloading and extracting census decennial and cencus acs")
    # census_etl.get_population_data(data_path)
    print("Merging csv population data with shape files")
    census_etl.merge_csv_shp_file(data_path)


    # # Step 3: Transform data
    # print("Transforming data to GeoJSON format.")
    # census_etl.transform()

    #  #Step 4: Load transformed data
    # print("Loading transformed data into CSV and GeoJSON.")
    # census_etl.load()
