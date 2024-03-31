from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.prod.system.config import settings

engine_sync = create_engine(
    url=settings.DATABASE_URL_psycopg,
    echo=False,
    # pool_size=5,
    # max_overflow=10,
)
session_sync = sessionmaker(engine_sync)




