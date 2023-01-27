import polars as pl
from app.models.pydantic import PayloadSchema

def retrieve_and_make_features(payload: PayloadSchema) -> pl.DataFrame:
    pass
