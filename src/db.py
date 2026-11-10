"""Database connections: MySQL (staging) and PostgreSQL (analytics)."""
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

load_dotenv()  # reads the .env file when I run outside Docker


def get_setting(name):
    """Read one setting from the environment. Stop with a clear message if it is missing."""
    value = os.getenv(name)
    if not value:
        raise ValueError(name + " is missing! Put it in your .env file.")
    return value


def get_mysql_engine():
    """Connection to the MySQL staging database."""
    url = URL.create(
        "mysql+pymysql",
        username=get_setting("MYSQL_USER"),
        password=get_setting("MYSQL_PASSWORD"),
        host=get_setting("MYSQL_HOST"),
        port=int(get_setting("MYSQL_PORT")),
        database=get_setting("MYSQL_DATABASE"),
    )
    return create_engine(url)


def get_postgres_engine():
    """Connection to the PostgreSQL analytics database."""
    url = URL.create(
        "postgresql+psycopg2",
        username=get_setting("POSTGRES_USER"),
        password=get_setting("POSTGRES_PASSWORD"),
        host=get_setting("POSTGRES_HOST"),
        port=int(get_setting("POSTGRES_PORT")),
        database=get_setting("POSTGRES_DATABASE"),
    )
    return create_engine(url)
