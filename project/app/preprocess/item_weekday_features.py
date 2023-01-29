import polars as pl
from typing import List

from app.preprocess.utils import freemem
from app.api.crud import retrieve_item_weekday_features

# input candidate_aid, weekday
async def get_item_weekday_features(
    candidate_aids: List, weekdays: List
) -> pl.DataFrame:
    results = await retrieve_item_weekday_features(
        candidate_aids=candidate_aids, weekdays=weekdays
    )

    # parse the result
    aids = []
    weekday_list = []
    itemXweekday_all_events_counts = []
    itemXweekday_cart_counts = []
    itemXweekday_click_to_cart_cvrs = []
    itemXweekday_cart_to_order_cvrs = []
    itemXweekday_frac_click_all_click_counts = []
    itemXweekday_frac_cart_all_cart_counts = []
    itemXweekday_frac_cart_all_weekday_cart_counts = []

    for row in results:
        aids.append(row["aid"])
        weekday_list.append(weekdays[0])
        itemXweekday_all_events_counts.append(row["itemXweekday_all_events_count"])
        itemXweekday_cart_counts.append(row["itemXweekday_cart_count"])
        itemXweekday_click_to_cart_cvrs.append(row["itemXweekday_click_to_cart_cvr"])
        itemXweekday_cart_to_order_cvrs.append(row["itemXweekday_cart_to_order_cvr"])
        itemXweekday_frac_click_all_click_counts.append(
            row["itemXweekday_frac_click_all_click_count"]
        )
        itemXweekday_frac_cart_all_cart_counts.append(
            row["itemXweekday_frac_cart_all_cart_count"]
        )
        itemXweekday_frac_cart_all_weekday_cart_counts.append(
            row["itemXweekday_frac_cart_all_weekday_cart_count"]
        )

    # save data as dataframe
    data = {
        "aid": aids,
        "weekday": weekday_list,
        "itemXweekday_all_events_count": itemXweekday_all_events_counts,
        "itemXweekday_cart_count": itemXweekday_cart_counts,
        "itemXweekday_click_to_cart_cvr": itemXweekday_click_to_cart_cvrs,
        "itemXweekday_cart_to_order_cvr": itemXweekday_cart_to_order_cvrs,
        "itemXweekday_frac_click_all_click_count": itemXweekday_frac_click_all_click_counts,
        "itemXweekday_frac_cart_all_cart_count": itemXweekday_frac_cart_all_cart_counts,
        "itemXweekday_frac_cart_all_weekday_cart_count": itemXweekday_frac_cart_all_weekday_cart_counts,
    }
    data_agg = pl.DataFrame(data)
    data_agg = freemem(data_agg)

    return data_agg
