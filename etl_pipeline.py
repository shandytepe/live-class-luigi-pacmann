import pandas as pd 
import luigi 
from helper.db_connector import postgres_engine_hotel, postgres_engine_load
from helper.scraper_helper import init_scrapper_engine, scrape_myanimelist, concat_anime_data
import requests
from tqdm import tqdm
import time

class ExtractHotelDatabase(luigi.Task):
   
    def requires(self):
        pass

    def run(self):
        # init postgres engine
        engine = postgres_engine_hotel()

        # query for fetch data from table
        query = "SELECT * FROM hotel_bookings"

        # read data from database using pandas
        extract_hotel = pd.read_sql(sql = query,
                                    con = engine)
        
        # export to csv
        extract_hotel.to_csv(self.output().path, index = False)
        
    def output(self):
        return luigi.LocalTarget("data/raw/extract_hotel_data.csv")
    
class ExtractAnimeData(luigi.Task):

    year = luigi.IntParameter(default = 2024)
    season = luigi.Parameter(default = "winter")

    def requires(self):
        pass

    def run(self):
        # init engine scrapper
        engine_scrapper = init_scrapper_engine(year = self.year,
                                               season = self.season)

        # scrape anime tv data
        get_anime_tv = scrape_myanimelist(soup = engine_scrapper,
                                          html_tag = "js-anime-category-producer seasonal-anime js-seasonal-anime js-anime-type-all js-anime-type-1",
                                          show_type = "TV")
        
        # scrape anime ona data
        get_anime_ona = scrape_myanimelist(soup = engine_scrapper,
                                        html_tag = "js-anime-category-producer seasonal-anime js-seasonal-anime js-anime-type-all js-anime-type-5",
                                        show_type = "ONA")
        
        # scrape anime ova data
        get_anime_ova = scrape_myanimelist(soup = engine_scrapper,
                                        html_tag = "js-anime-category-producer seasonal-anime js-seasonal-anime js-anime-type-all js-anime-type-2",
                                        show_type = "OVA")
        
        # scrape anime movie data
        get_anime_movie = scrape_myanimelist(soup = engine_scrapper,
                                            html_tag = "js-anime-category-producer seasonal-anime js-seasonal-anime js-anime-type-all js-anime-type-3",
                                            show_type = "Movies")
        
        # concat data
        concat_data = concat_anime_data(anime_data = [get_anime_tv, get_anime_ona,
                                                      get_anime_ova, get_anime_movie])
        
        # save the output to csv
        concat_data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget(f"data/raw/extract_anime_{self.year}_{self.season}.csv")
    
class ExtractAPIMangaData(luigi.Task):
    
    def requires(self):
        pass

    def run(self):
        full_data = []

        for num_id in tqdm(range(1, 201)):
            
            # hit the API
            resp = requests.get(f"https://api.jikan.moe/v4/manga/{num_id}/full")

            # create branching, if API didn't send the result we pass it
            if resp.status_code == 404:
                pass

            # if there's a 200 status code, we start get the data
            else:
                raw_data = resp.json()

                manga_data = {
                    "manga_id": raw_data["data"]["mal_id"],
                    "url_manga": raw_data["data"]["url"],
                    "title": raw_data["data"]["title"],
                    "title_english": raw_data["data"]["title_english"],
                    "title_japanese": raw_data["data"]["title_japanese"],
                    "chapters": raw_data["data"]["chapters"],
                    "volumes": raw_data["data"]["volumes"],
                    "status": raw_data["data"]["status"],
                    "start_published": raw_data["data"]["published"]["from"],
                    "end_published": raw_data["data"]["published"]["to"],
                    "score": raw_data["data"]["score"],
                    "rank": raw_data["data"]["rank"],
                    "authors": [entry["name"] for entry in raw_data["data"]["authors"]],
                    "genres": [entry["name"] for entry in raw_data["data"]["genres"]],
                    "themes": [entry["name"] for entry in raw_data["data"]["themes"]]
                }

                full_data.append(manga_data)

            time.sleep(1)

        # convert to DataFrame
        manga_data = pd.DataFrame(full_data)

        # export to csv
        manga_data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget("data/raw/extract_manga_data.csv")
    
# optional pipeline
class ValidateData(luigi.Task):

    def requires(self):
        return [ExtractHotelDatabase(), ExtractAnimeData(), ExtractAPIMangaData()]
    
    def run(self):

        for idx in range(0, 3):

            data = pd.read_csv(self.input()[idx].path)

            # start data quality pipeline
            print("===== Data Quality Pipeline Start =====")
            print("")

            # check data shape
            print("===== Check Data Shape =====")
            print("")
            print(f"Data Shape for this Data {data.shape}")

            # check data type
            get_cols = data.columns

            print("")
            print("===== Check Data Types =====")
            print("")

            # iterate to each column
            for col in get_cols:
                print(f"Column {col} has data type {data[col].dtypes}")

            # check missing values
            print("")
            print("===== Check Missing Values =====")
            print("")

            # iterate to each column
            for col in get_cols:

                # calculate missing values in percentage
                get_missing_values = (data[col].isnull().sum() * 100) / len(data)
                print(f"Columns {col} has percentages missing values {get_missing_values} %")

            print("===== Data Quality Pipeline End =====")
            print("")

            data.to_csv(self.output()[idx].path, index = False)

    def output(self):
        return [luigi.LocalTarget("data/validate/validate_hotel_data.csv"),
                luigi.LocalTarget("data/validate/validate_anime_2024_winter_data.csv"),
                luigi.LocalTarget("data/validate/validate_manga_data.csv")]
    
class TransformHotelData(luigi.Task):
    
    def requires(self):
        return ValidateData()
    
    def run(self):
        # read data from previous task
        transform_hotel_data = pd.read_csv(self.input()[0].path)

        # fill the children column with 0 values before convert to int
        transform_hotel_data["children"] = transform_hotel_data["children"].fillna(value = 0)
        
        # convert children column to int
        transform_hotel_data["children"] = transform_hotel_data["children"].astype(int)

        # drop agent and company columns
        transform_hotel_data.drop(["agent", "company"], axis = 1, inplace = True)

        # convert data to datetime data type
        transform_hotel_data["reservation_status_date"] = pd.to_datetime(transform_hotel_data["reservation_status_date"])
        transform_hotel_data["arrival_date"] = pd.to_datetime(transform_hotel_data["arrival_date"])

        # init value to convert the values
        CONVERT_VALUES = {
            0: False,
            1: True
        }

        # init columns that we want to convert the values
        COLS_TO_CONVERT = ["is_canceled", "is_repeated_guest"]

        for col in COLS_TO_CONVERT:
            print(f"Start Convert values Column {col}")
            transform_hotel_data[col] = transform_hotel_data[col].replace(CONVERT_VALUES)
            print("End of Process")

        # export to csv
        transform_hotel_data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget("data/transform/transform_hotel_data.csv")

class TransformAnimeData(luigi.Task):
    
    def requires(self):
        return ValidateData()
    
    def run(self):
        # read data from the previous task
        transform_anime_data = pd.read_csv(self.input()[1].path)

        # convert data to datetime data type
        transform_anime_data["release_date"] = pd.to_datetime(transform_anime_data["release_date"])

        # replace the values for anime episode based on the requirements
        transform_anime_data["anime_episode"] = transform_anime_data["anime_episode"].replace("? eps", 0)

        # extract only numeric from anime episode and duration columns
        transform_anime_data["anime_episode"] = transform_anime_data["anime_episode"].str.extract('(\d+)')
        transform_anime_data["anime_duration"] = transform_anime_data["anime_duration"].str.extract('(\d+)')  

        # remove squared bracket from anime genre column
        transform_anime_data["anime_genre"] = transform_anime_data["anime_genre"].str.strip("[]")         

        # export to csv
        transform_anime_data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget("data/transform/transform_anime_data.csv")

class TransformMangaData(luigi.Task):

    def requires(self):
        return ValidateData()
    
    def run(self):
        # read data from previous task
        transform_manga_data = pd.read_csv(self.input()[2].path)

        # fill the missing values with "Not Found" value
        transform_manga_data["chapters"] = transform_manga_data["chapters"].fillna(value = "Not Found")
        transform_manga_data["volumes"] = transform_manga_data["volumes"].fillna(value = "Not Found")

        # fill the missing values with 0 value
        transform_manga_data["score"] = transform_manga_data["score"].fillna(value = 0)
        transform_manga_data["rank"] = transform_manga_data["rank"].fillna(value = 0)

        # convert the columns to datetime data type
        transform_manga_data["start_published"] = pd.to_datetime(transform_manga_data["start_published"])
        transform_manga_data["end_published"] = pd.to_datetime(transform_manga_data["end_published"])

        # convert rank column to int
        transform_manga_data["rank"] = transform_manga_data["rank"].astype(int)

        # export to csv
        transform_manga_data.to_csv(self.output().path, index = False)


    def output(self):
        return luigi.LocalTarget("data/transform/transform_manga_data.csv")
    
class LoadData(luigi.Task):
    
    def requires(self):
        return [TransformHotelData(),
                TransformAnimeData(),
                TransformMangaData()]
    
    def output(self):
        return [luigi.LocalTarget("data/load/load_hotel_data.csv"),
                luigi.LocalTarget("data/load/load_anime_data.csv"),
                luigi.LocalTarget("data/load/load_manga_data.csv")]
    
    def run(self):
        # init postgres engine load
        engine = postgres_engine_load()

        # read data from previous task
        load_hotel_data = pd.read_csv(self.input()[0].path)
        load_anime_data = pd.read_csv(self.input()[1].path)
        load_manga_data = pd.read_csv(self.input()[2].path)

        # init table name for each task
        hotel_data_name = "hotel_data"
        anime_data_name = "anime_data"
        manga_data_name = "manga_data"

        # insert to database
        load_hotel_data.to_sql(name = hotel_data_name,
                               con = engine,
                               if_exists = "append",
                               index = False)
        
        load_anime_data.to_sql(name = anime_data_name,
                               con = engine,
                               if_exists = "append",
                               index = False)
        
        load_manga_data.to_sql(name = manga_data_name,
                               con = engine,
                               if_exists = "append",
                               index = False)
        
        # save the process
        load_hotel_data.to_csv(self.output()[0].path, index = False)
        load_anime_data.to_csv(self.output()[1].path, index = False)
        load_manga_data.to_csv(self.output()[2].path, index = False)

if __name__ == "__main__":
    luigi.build([ExtractHotelDatabase(),
                 ExtractAnimeData(year = 2024, season = "winter"),
                 ExtractAPIMangaData(),
                 ValidateData(),
                 TransformHotelData(),
                 TransformAnimeData(),
                 TransformMangaData(),
                 LoadData()])