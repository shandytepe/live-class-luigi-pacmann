from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_USERNAME_HOTEL = os.getenv("DB_USERNAME_HOTEL")
DB_PASSWORD_HOTEL = os.getenv("DB_PASSWORD_HOTEL")
DB_HOST_HOTEL = os.getenv("DB_HOST_HOTEL")
DB_NAME_HOTEL = os.getenv("DB_NAME_HOTEL")

DB_USERNAME_LOAD = os.getenv("DB_USERNAME_LOAD")
DB_PASSWORD_LOAD = os.getenv("DB_PASSWORD_LOAD")
DB_HOST_LOAD = os.getenv("DB_HOST_LOAD")
DB_NAME_LOAD = os.getenv("DB_NAME_LOAD")

def postgres_engine_hotel():
    engine = create_engine(f"postgresql://{DB_USERNAME_HOTEL}:{DB_PASSWORD_HOTEL}@{DB_HOST_HOTEL}/{DB_NAME_HOTEL}")

    return engine

def postgres_engine_load():
    engine = create_engine(f"postgresql://{DB_USERNAME_LOAD}:{DB_PASSWORD_LOAD}@{DB_HOST_LOAD}/{DB_NAME_LOAD}")

    return engine