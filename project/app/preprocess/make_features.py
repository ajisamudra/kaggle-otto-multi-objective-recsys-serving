from typing import List

import polars as pl

from app.models.pydantic import PayloadSchema
from app.retrieval.all_retrieval import retrieve_candidates


def make_features(payload: PayloadSchema, candidates: pl.DataFrame) -> pl.DataFrame:
    # make candidate df
    # data = {"candidate_aid": candidates, "features1": candidates}
    # cand_df = pl.DataFrame(data)

    # session features

    # session-item features

    # item-hour features

    # item-weekday features

    # item-word2vec features

    # item-covisit features

    return candidates
