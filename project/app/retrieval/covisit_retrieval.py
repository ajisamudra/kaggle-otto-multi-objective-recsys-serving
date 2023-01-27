from typing import List, Tuple

import polars as pl

from app.models.pydantic import PayloadSchema


def covisit_retrieval(payload: PayloadSchema) -> Tuple[List, List]:
    return [9999, 5555, 1234], [0, 1, 2]
