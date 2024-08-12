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


# Constants
#BASE_URL_FIPS = "https://www.census.gov/geographies/reference-files/time-series/geo/tallies.html"
#BASE_URL_FIPS = "https://www2.census.gov/geo/docs/maps-data/data/geo_tallies/census_block_tally.txt"
BASE_URL_FIPS="https://www2.census.gov/geo/docs/maps-data/data/geo_tallies2020/2020talliesbystate.xlsx"

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

  

    def __init__(self):
        self.shape_file_path = Path("data/census/shp")
        self.STATE_FIPS_CODES = []
        self.TRACT_PER_STATE = {}  # in-memory dict per state
        self.TRACT_NATIONAL = []  # in-memory global list
    
    def fetch_and_extract_fips_codes(self, data_path: Path):
        """Fetches and extracts FIPS codes."""
        print(f"Fetching FIPS codes from URL: {BASE_URL_FIPS}")
        
        fips_xlsx_path = data_path / "census" / "csv" /"census_block_tally.xlsx"
        fips_csv_path = data_path / "census" / "csv" /"census_block_tally.csv"
        fips_xlsx_path.parent.mkdir(parents=True, exist_ok=True)
        Downloader.download_file_from_url(BASE_URL_FIPS, fips_xlsx_path)
        
        # Check if the file exists and log
        if not fips_xlsx_path.is_file():
            sys.exit(f"FIPS CSV file not found at path {fips_xlsx_path}")
        print(f"Reading FIPS codes from {fips_xlsx_path}")

        #df = pd.read_excel(fips_xlsx_path, engine='openpyxl')
    
    # Save to CSV
        #df.to_csv(fips_csv_path, index=False)
        # with open(fips_xlsx_path, encoding="utf-8") as csv_file:
        #     csv_reader = csv.reader(csv_file, delimiter=",")
        #     next(csv_reader)  # Skip header
        #     for row in csv_reader:
        #         fips_code = row[0].strip()
        #         self.STATE_FIPS_CODES.append(fips_code)
             
        df = pd.read_excel(fips_xlsx_path, engine='openpyxl',dtype=str)

        
        statefp_column = df['STATEFP'].astype(str).tolist()
       

        print(type(statefp_column[0]))
        for fips_code in statefp_column:
            if fips_code!="nan":
                self.STATE_FIPS_CODES.append((fips_code))
                print(f"Fetched FIPS code: {fips_code}")



        
        # print(f"Reading FIPS codes from {fips_csv_path}")
        # with open(fips_csv_path, encoding="utf-8") as file:
        # #    next(file)
        #     csv_reader = csv.reader(file, delimiter=",")
        #     next(csv_reader)  # Skip header
        #     for row in csv_reader:
        #         if row and len(row) > 0:
        #             fips_code = row[0].strip()
        #      #   self.STATE_FIPS_CODES.append(fips_code)
        #    # for fip in self.STATE_FIPS_CODES:
        #     #    print(f"Fetched FIPS code: {fip}")
        #     #for line in file:
        #      #   columns = [col.strip() for col in line.split("\t")]
        #     #if len(columns) > 0:
        #      #   fips_code = columns[0]
        #       #  print("/////////////////fips_code", fips_code)
        #             if fips_code.isdigit() and len(fips_code)== 2:
        #                 self.STATE_FIPS_CODES.append(fips_code)
        # for fip in self.STATE_FIPS_CODES:
        #     print(f"Fetched FIPS code: {fip}")
        

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
    def fetch(self,source: str,destination:Path)->Path:

        destination.mkdir(parents=True, exist_ok=True)
        Downloader.download_zip_file_from_url(
            file_url=source,
            unzipped_file_path=destination,
            verify=True,
        )
        return destination




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





    def _transform_to_geojson(self, fips_code: str) -> None:
        """Transforms a shapefile to GeoJSON format."""
        shp_file_path = self._path_for_fips_file(fips_code, GeoFileType.SHP)
        geojson_file_path = self._path_for_fips_file(fips_code, GeoFileType.GEOJSON)

        print(f"Transforming shapefile to GeoJSON for FIPS code: {fips_code}")
        if not geojson_file_path.is_file():
            cmd = ["ogr2ogr", "-f", "GeoJSON", str(geojson_file_path), str(shp_file_path)]
            try:
                subprocess.run(cmd, check=True, capture_output=True, text=True)
                print(f"Transformation complete: {geojson_file_path}")
            except subprocess.CalledProcessError as e:
                print(f"Command '{e.cmd}' returned non-zero exit status {e.returncode}.")
                print(f"Standard Output: {e.stdout}")
                print(f"Standard Error: {e.stderr}")

    def _generate_tract_table(self) -> None:
        """Generates a table of tract IDs from GeoJSON files."""
        print("Generating national tract table from GeoJSON files.")
        for file in self.GEOJSON_BASE_PATH.rglob("*.json"):
            print(f"Processing file: {file}")
            with open(file, encoding="utf-8") as f:
                geojson = json.load(f)
                for feature in geojson["features"]:
                    tractid10 = feature["properties"].get("GEOID10", None)
                    if tractid10:
                        self.TRACT_NATIONAL.append(str(tractid10))
                        tractid10_state_id = tractid10[:2]
                        if not self.TRACT_PER_STATE.get(tractid10_state_id):
                            self.TRACT_PER_STATE[tractid10_state_id] = []
                        self.TRACT_PER_STATE[tractid10_state_id].append(tractid10)
                        print(f"Processed tract ID: {tractid10}")

    def transform(self) -> None:
        """Executes the transformation process."""
        for index, fips_code in enumerate(self.STATE_FIPS_CODES):
            print(f"Transforming FIPS {fips_code} to GeoJSON – {index+1} of {len(self.STATE_FIPS_CODES)}")
            self._transform_to_geojson(fips_code)
        self._generate_tract_table()

    def _load_into_state_csvs(self, fips_code: str) -> None:
        """Loads tract IDs into CSV files for each state."""
        tractid10_list = self.TRACT_PER_STATE.get(fips_code, [])
        csv_path = self._path_for_fips_file(fips_code, GeoFileType.CSV)
        print(f"Loading tract IDs into CSV for FIPS code: {fips_code}")
        with open(csv_path, mode="w", newline="", encoding="utf-8") as cbg_csv_file:
            tract_csv_file_writer = csv.writer(cbg_csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for tractid10 in tractid10_list:
                tract_csv_file_writer.writerow([tractid10])
                print(f"Written tract ID to CSV: {tractid10}")

    def _load_national_csv(self):
        """Loads national tract IDs into a CSV file."""
        print("Loading national tract IDs into CSV.")
        if not self.NATIONAL_TRACT_CSV_PATH.is_file():
            with open(self.NATIONAL_TRACT_CSV_PATH, mode="w", newline="", encoding="utf-8") as cbg_csv_file:
                cbg_csv_file_writer = csv.writer(cbg_csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for geoid10 in self.TRACT_NATIONAL:
                    cbg_csv_file_writer.writerow([geoid10])
                    print(f"Written national tract ID to CSV: {geoid10}")

    def _load_national_geojson(self):
        """Combines all GeoJSON files into a national GeoJSON file."""
        print("Combining GeoJSON files into a national GeoJSON.")
        usa_df = gpd.GeoDataFrame()

        for file_name in self.GEOJSON_BASE_PATH.rglob("*.json"):
            print(f"Reading GeoJSON file: {file_name}")
            state_gdf = gpd.read_file(file_name)
            usa_df = usa_df.append(state_gdf)

        print("Reprojecting to WGS84 coordinate system.")
        usa_df = usa_df.to_crs("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")
        usa_df.to_file(self.NATIONAL_TRACT_JSON_PATH, driver="GeoJSON")
        print(f"National GeoJSON file created: {self.NATIONAL_TRACT_JSON_PATH}")

    def load(self) -> None:
        """Executes the loading process."""
        print("Loading data into CSV and GeoJSON files.")
        for fips_code in self.TRACT_PER_STATE:
            self._load_into_state_csvs(fips_code)
        self._load_national_csv()
        self._load_national_geojson()
        print("Census data complete")

if __name__ == "__main__":
    data_path = Path("data")
    downloader = Downloader()
    census_etl = CensusETL()

    # Step 1: Fetch and extract FIPS codes
    print("Starting FIPS codes extraction.")
    census_etl.fetch_and_extract_fips_codes(data_path)

    # Step 2: Download zip files and extract them
    print("Downloading and extracting shapefiles.")
    for url, destination in census_etl.get_data_sources():
        print("url", url)
        #print("Downloading from", url, "to",destination)
        print("Destination", destination)
        downloader.download_zip_file_from_url(url, destination)

    # # Step 3: Transform data
    # print("Transforming data to GeoJSON format.")
    # census_etl.transform()

    #  #Step 4: Load transformed data
    # print("Loading transformed data into CSV and GeoJSON.")
    # census_etl.load()
