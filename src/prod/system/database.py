from typing_extensions import Annotated
from sqlalchemy import create_engine, String
from sqlalchemy.orm import sessionmaker, registry, DeclarativeBase
from src.prod.system.config import settings

engine_sync = create_engine(
    url=settings.DATABASE_URL_psycopg,
    echo=False,
)
session_sync = sessionmaker(engine_sync)

str_3 = Annotated[str, 3]
str_64 = Annotated[str, 64]


class Base(DeclarativeBase):
    registry = registry(
        type_annotation_map={
            str_3: String(3),
            str_64: String(64),
        }
    )
