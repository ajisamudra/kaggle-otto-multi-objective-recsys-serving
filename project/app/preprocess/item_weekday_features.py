import logging
from typing import List

import numpy as np
import polars as pl

from app.api.crud import retrieve_item_weekday_features
from app.preprocess.utils import freemem

log = logging.getLogger("uvicorn")

# input candidate_aid, weekday
async def get_item_weekday_features(
    candidate_aids: List, weekdays: List
) -> pl.DataFrame:
    results = await retrieve_item_weekday_features(
        candidate_aids=candidate_aids, weekdays=weekdays
    )

    data = np.array(results, dtype=np.float32)
    columns = [
        "aid",
        "weekday",
        "itemXweekday_all_events_count",
        "itemXweekday_cart_count",
        "itemXweekday_click_to_cart_cvr",
        "itemXweekday_cart_to_order_cvr",
        "itemXweekday_frac_click_all_click_count",
        "itemXweekday_frac_cart_all_cart_count",
        "itemXweekday_frac_cart_all_weekday_cart_count",
    ]
    data_agg = pl.DataFrame(data=data, columns=columns)
    data_agg = data_agg.with_columns(
        [
            pl.col("aid").cast(pl.Int32),
            pl.col("weekday").cast(pl.Int32),
        ]
    )
    data_agg = freemem(data_agg)

    return data_agg
