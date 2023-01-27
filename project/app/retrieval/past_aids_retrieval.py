from typing import List, Tuple

import polars as pl

from app.data_models.pydantic import PayloadSchema


def past_aids_retrieval(payload: PayloadSchema) -> Tuple[List, List]:
    unique_aids = list(dict.fromkeys(payload.aids[::-1]))
    ranks = [i for i in range(len(unique_aids))]
    return unique_aids, ranks
