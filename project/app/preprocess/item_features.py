import polars as pl
from typing import List

from app.preprocess.utils import freemem
from app.api.crud import retrieve_item_features

# input candidate_aid, hour
async def get_item_features(candidate_aids: List) -> pl.DataFrame:
    results = await retrieve_item_features(candidate_aids=candidate_aids)

    # parse the result
    aids = []
    item_all_events_counts = []
    item_click_counts = []
    item_cart_counts = []
    item_order_counts = []
    item_avg_hour_clicks = []
    item_avg_hour_carts = []
    item_avg_hour_orders = []
    item_avg_weekday_clicks = []
    item_avg_weekday_orders = []

    for row in results:
        aids.append(row["aid"])
        item_all_events_counts.append(row["item_all_events_count"])
        item_click_counts.append(row["item_click_count"])
        item_cart_counts.append(row["item_cart_count"])
        item_order_counts.append(row["item_order_count"])
        item_avg_hour_clicks.append(row["item_avg_hour_click"])
        item_avg_hour_carts.append(row["item_avg_hour_cart"])
        item_avg_hour_orders.append(row["item_avg_hour_order"])
        item_avg_weekday_clicks.append(row["item_avg_weekday_click"])
        item_avg_weekday_orders.append(row["item_avg_weekday_order"])

    # save data as dataframe
    data = {
        "aid": aids,
        "item_all_events_count": item_all_events_counts,
        "item_click_count": item_click_counts,
        "item_cart_count": item_cart_counts,
        "item_order_count": item_order_counts,
        "item_avg_hour_click": item_avg_hour_clicks,
        "item_avg_hour_cart": item_avg_hour_carts,
        "item_avg_hour_order": item_avg_hour_orders,
        "item_avg_weekday_click": item_avg_weekday_clicks,
        "item_avg_weekday_order": item_avg_weekday_orders,
    }

    data_agg = pl.DataFrame(data)
    data_agg = freemem(data_agg)

    return data_agg
