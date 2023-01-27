import polars as pl

from app.preprocess.utils import freemem


# input candidate_aid, weekday
def get_item_weekday_features(cand_df: pl.DataFrame, weekday: pl.Series):

    # dummy data
    data = {
        "aid": [i for i in range(100)],
        "weekday": [i for i in range(100)],
        "itemXweekday_all_events_count": [i for i in range(100)],
        "itemXweekday_cart_count": [i for i in range(100)],
        "itemXweekday_click_to_cart_cvr": [0.1 for i in range(100)],
        "itemXweekday_cart_to_order_cvr": [0.05 for i in range(100)],
        "itemXweekday_frac_click_all_click_count": [0.5 for i in range(100)],
        "itemXweekday_frac_cart_all_cart_count": [0.1 for i in range(100)],
        "itemXweekday_frac_cart_all_weekday_cart_count": [0.2 for i in range(100)],
    }
    data_agg = pl.DataFrame(data)
    data_agg = freemem(data_agg)

    return data_agg
