from typing import List

import polars as pl

from app.models.pydantic import PayloadSchema
from app.preprocess.make_features import make_features
from app.retrieval.all_retrieval import retrieve_candidates


def retrieve_and_make_features(payload: PayloadSchema) -> pl.DataFrame:
    # retrieve candidates
    candidates = retrieve_candidates(payload=payload)

    # make features for the candidates
    df_features = make_features(candidates=candidates, payload=payload)

    return df_features
