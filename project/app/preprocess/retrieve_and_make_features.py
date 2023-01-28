import polars as pl
from starlette.requests import Request

from app.data_models.pydantic import PayloadSchema
from app.preprocess.make_features import make_features
from app.retrieval.all_retrieval import retrieve_candidates


def retrieve_and_make_features(
    request: Request, payload: PayloadSchema
) -> pl.DataFrame:
    # retrieve candidates
    candidates = retrieve_candidates(payload=payload, request=request)

    # make features for the candidates
    df_features = make_features(candidates=candidates, payload=payload, request=request)

    return df_features
