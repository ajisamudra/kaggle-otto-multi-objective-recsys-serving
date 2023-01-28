import polars as pl

from app.preprocess.utils import freemem


# input candidate_aid, hour
def get_item_features(cand_df: pl.DataFrame):

    # dummy data
    data = {
        "aid": [i for i in range(100)],
        "item_all_events_count": [i for i in range(100)],
        "item_click_count": [i for i in range(100)],
        "item_cart_count": [i for i in range(100)],
        "item_order_count": [i for i in range(100)],
        "item_avg_hour_click": [i for i in range(100)],
        "item_avg_hour_cart": [i for i in range(100)],
        "item_avg_hour_order": [i for i in range(100)],
        "item_avg_weekday_click": [i for i in range(100)],
        "item_avg_weekday_order": [i for i in range(100)],
    }

    data_agg = pl.DataFrame(data)
    data_agg = freemem(data_agg)

    return data_agg
