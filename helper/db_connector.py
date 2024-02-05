from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# load .env file
load_dotenv()

# initiate variable to get env values for hotel database
DB_USERNAME_HOTEL = os.getenv("DB_USERNAME_HOTEL")
DB_PASSWORD_HOTEL = os.getenv("DB_PASSWORD_HOTEL")
DB_HOST_HOTEL = os.getenv("DB_HOST_HOTEL")
DB_NAME_HOTEL = os.getenv("DB_NAME_HOTEL")

# initiate variable to get env to load database recommender system
DB_USERNAME_LOAD = os.getenv("DB_USERNAME_LOAD")
DB_PASSWORD_LOAD = os.getenv("DB_PASSWORD_LOAD")
DB_HOST_LOAD = os.getenv("DB_HOST_LOAD")
DB_NAME_LOAD = os.getenv("DB_NAME_LOAD")

def postgres_engine_hotel():
    """
    Function yang digunakan untuk membuat engine postgres
    yang tujuannya untuk fetch data dari hotel database.
    Sesuaikan username, password, host, dan nama database dengan milik masing - masing
    """
    engine = create_engine(f"postgresql://{DB_USERNAME_HOTEL}:{DB_PASSWORD_HOTEL}@{DB_HOST_HOTEL}/{DB_NAME_HOTEL}")

    return engine

def postgres_engine_load():
    """
    Function yang digunakan untuk membuat engine postgres
    yang tujuannya untuk load data ke recommender system database.
    Sesuaikan username, password, host, dan nama database dengan milik masing - masing
    """
    engine = create_engine(f"postgresql://{DB_USERNAME_LOAD}:{DB_PASSWORD_LOAD}@{DB_HOST_LOAD}/{DB_NAME_LOAD}")

    return engine