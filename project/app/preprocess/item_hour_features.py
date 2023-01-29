import polars as pl
from typing import List

from app.preprocess.utils import freemem
from app.api.crud import retrieve_item_hour_features


# input candidate_aid, hour
async def get_item_hour_features(candidate_aids: List, hours: List) -> pl.DataFrame:
    results = await retrieve_item_hour_features(
        candidate_aids=candidate_aids, hours=hours
    )

    # parse the result
    aids = []
    hour_list = []
    itemXhour_click_counts = []
    itemXhour_click_to_cart_cvrs = []
    itemXhour_frac_click_all_click_counts = []
    itemXhour_frac_click_all_hour_click_counts = []

    for row in results:
        aids.append(row["aid"])
        hour_list.append(hours[0])
        itemXhour_click_counts.append(row["itemXhour_click_count"])
        itemXhour_click_to_cart_cvrs.append(row["itemXhour_click_to_cart_cvr"])
        itemXhour_frac_click_all_click_counts.append(
            row["itemXhour_frac_click_all_click_count"]
        )
        itemXhour_frac_click_all_hour_click_counts.append(
            row["itemXhour_frac_click_all_hour_click_count"]
        )

    # save data as dataframe
    data = {
        "aid": aids,
        "hour": hour_list,
        "itemXhour_click_count": itemXhour_click_counts,
        "itemXhour_click_to_cart_cvr": itemXhour_click_to_cart_cvrs,
        "itemXhour_frac_click_all_click_count": itemXhour_frac_click_all_click_counts,
        "itemXhour_frac_click_all_hour_click_count": itemXhour_frac_click_all_hour_click_counts,
    }

    data_agg = pl.DataFrame(data)
    data_agg = freemem(data_agg)

    return data_agg
