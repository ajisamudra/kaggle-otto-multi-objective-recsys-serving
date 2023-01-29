import logging
import polars as pl
from typing import List
import numpy as np

from app.preprocess.utils import freemem
from app.api.crud import retrieve_item_hour_features

log = logging.getLogger("uvicorn")

# input candidate_aid, hour
async def get_item_hour_features(candidate_aids: List, hours: List) -> pl.DataFrame:
    results = await retrieve_item_hour_features(
        candidate_aids=candidate_aids, hours=hours
    )

    data = np.array(results, dtype=np.float32)
    columns = [
        "aid",
        "hour",
        "itemXhour_click_count",
        "itemXhour_click_to_cart_cvr",
        "itemXhour_frac_click_all_click_count",
        "itemXhour_frac_click_all_hour_click_count",
    ]
    data_agg = pl.DataFrame(data=data, columns=columns)
    data_agg = data_agg.with_columns(
        [
            pl.col("aid").cast(pl.Int32),
            pl.col("hour").cast(pl.Int32),
        ]
    )

    data_agg = freemem(data_agg)

    return data_agg
