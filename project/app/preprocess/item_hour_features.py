import polars as pl

from app.preprocess.utils import freemem


# input candidate_aid, hour
def get_item_hour_features(cand_df: pl.DataFrame, hour: pl.Series):

    # dummy data
    data = {
        "aid": [i for i in range(100)],
        "hour": [i for i in range(100)],
        "itemXhour_click_count": [i for i in range(100)],
        "itemXhour_click_to_cart_cvr": [0.5 for i in range(100)],
        "itemXhour_frac_click_all_click_count": [0.2 for i in range(100)],
        "itemXhour_frac_click_all_hour_click_count": [0.1 for i in range(100)],
    }

    data_agg = pl.DataFrame(data)
    data_agg = freemem(data_agg)

    return data_agg
