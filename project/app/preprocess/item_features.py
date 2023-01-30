import logging
from typing import List

import numpy as np
import polars as pl

from app.api.crud import retrieve_item_features
from app.preprocess.utils import freemem

log = logging.getLogger("uvicorn")


async def get_item_features(candidate_aids: List) -> pl.DataFrame:
    results = await retrieve_item_features(candidate_aids=candidate_aids)

    data = np.array(results, dtype=np.float32)
    columns = [
        "aid",
        "item_all_events_count",
        "item_click_count",
        "item_cart_count",
        "item_order_count",
        "item_avg_hour_click",
        "item_avg_hour_cart",
        "item_avg_hour_order",
        "item_avg_weekday_click",
        "item_avg_weekday_order",
    ]
    data_agg = pl.DataFrame(data=data, columns=columns)
    data_agg = data_agg.with_columns(
        [
            pl.col("aid").cast(pl.Int32),
        ]
    )

    data_agg = freemem(data_agg)

    return data_agg
