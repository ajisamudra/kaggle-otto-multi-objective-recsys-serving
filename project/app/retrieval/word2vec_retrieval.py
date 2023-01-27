from typing import List, Tuple

import polars as pl

from app.models.pydantic import PayloadSchema


def word2vec_retrieval(payload: PayloadSchema) -> Tuple[List, List]:
    return [1000, 9998, 9996], [0, 1, 2]
