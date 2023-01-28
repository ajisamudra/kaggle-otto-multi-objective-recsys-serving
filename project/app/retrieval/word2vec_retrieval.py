from typing import List, Tuple

import polars as pl
from starlette.requests import Request

from app.data_models.pydantic import PayloadSchema
from app.embeddings.word2vec import suggest_clicks_word2vec


def word2vec_retrieval(payload: PayloadSchema, request: Request) -> Tuple[List, List]:
    # candidates, ranks = suggest_clicks_word2vec(
    #     n_candidate=10, aids=payload.aids, embedding=request.app.state.word2vec_idx
    # )
    candidates = [9999, 1234, 1235]
    ranks = [0, 1, 2]
    return candidates, ranks
