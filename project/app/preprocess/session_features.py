import numpy as np
import polars as pl

from app.preprocess.preprocess_events import get_hour_from_ts, get_weekday_from_ts
from app.preprocess.utils import freemem


def make_session_features(sess_df: pl.DataFrame):
    """
    df input from
    session | type | ts | aid
    123 | 0 | 12313 | AID1
    123 | 1 | 12314 | AID1
    123 | 2 | 12345 | AID1
    """

    sess_df = sess_df.with_columns(
        [
            pl.when(pl.col("type") == 0).then(1).otherwise(None).alias("dummy_click"),
            pl.when(pl.col("type") == 1).then(1).otherwise(None).alias("dummy_cart"),
            pl.when(pl.col("type") == 2).then(1).otherwise(None).alias("dummy_order"),
        ],
    )

    sess_df = sess_df.with_columns(
        [
            (pl.col("dummy_click") * pl.col("hour")).alias("hour_click"),
            (pl.col("dummy_cart") * pl.col("hour")).alias("hour_cart"),
            (pl.col("dummy_order") * pl.col("hour")).alias("hour_order"),
            (pl.col("dummy_click") * pl.col("weekday")).alias("weekday_click"),
            (pl.col("dummy_cart") * pl.col("weekday")).alias("weekday_cart"),
            (pl.col("dummy_order") * pl.col("weekday")).alias("weekday_order"),
        ],
    )

    # agg per session
    data_agg = sess_df.groupby("session").agg(
        [
            pl.col("aid").count().alias("sess_all_events_count"),
            pl.col("aid").n_unique().alias("sess_aid_dcount"),
            # num of event type
            (pl.col("type") == 0).sum().alias("sess_click_count"),
            (pl.col("type") == 1).sum().alias("sess_cart_count"),
            # aid dcount per event type
            pl.col("aid")
            .filter(pl.col("type") == 0)
            .n_unique()
            .fill_null(0)
            .alias("sess_clicked_aid_dcount"),
            pl.col("aid")
            .filter(pl.col("type") == 1)
            .n_unique()
            .fill_null(0)
            .alias("sess_carted_aid_dcount"),
            pl.col("aid")
            .filter(pl.col("type") == 2)
            .n_unique()
            .fill_null(0)
            .alias("sess_ordered_aid_dcount"),
            ((pl.col("ts").max() - pl.col("ts").min()) / 60).alias(
                "sess_duration_mins"
            ),
            pl.col("duration_second")
            .filter(pl.col("type") == 1)
            .mean()
            .fill_null(0)
            .alias("sess_avg_cart_dur_sec"),
            pl.col("hour_cart").mean().alias("sess_avg_hour_cart"),
            pl.col("weekday_cart").mean().alias("sess_avg_weekday_cart"),
            # for extracting hour and day of last event
            pl.col("ts").last().alias("curr_ts"),
            pl.col("type").last().alias("sess_last_type_in_session"),
        ]
    )

    data_agg = data_agg.with_columns(
        [
            pl.col("curr_ts").apply(lambda x: get_hour_from_ts(x)).alias("sess_hour"),
            pl.col("curr_ts")
            .apply(lambda x: get_weekday_from_ts(x))
            .alias("sess_weekday"),
        ],
    )

    data_agg = data_agg.with_columns(
        [
            (pl.col("sess_ordered_aid_dcount") / pl.col("sess_carted_aid_dcount"))
            .fill_nan(0)
            .fill_null(0)
            .alias("sess_carted_to_ordered_aid_cvr"),
            np.abs(pl.col("sess_hour") - pl.col("sess_avg_hour_cart"))
            .cast(pl.Int32)
            .fill_null(99)
            .alias("sess_abs_diff_avg_hour_cart"),
            np.abs(pl.col("sess_weekday") - pl.col("sess_avg_weekday_cart"))
            .cast(pl.Int32)
            .fill_null(99)
            .alias("sess_abs_diff_avg_weekday_cart"),
        ],
    )

    def binning_aid_dcount(x):
        if x <= 4:
            # return str(x)
            return x
        elif (x > 4) and (x <= 8):
            return 5  # "5_8"
        elif (x > 8) and (x <= 12):
            return 6  # "9_12"
        elif (x > 12) and (x <= 15):
            return 7  # "13_15"
        elif (x > 15) and (x <= 20):
            return 8  # "16_20"
        elif (x > 20) and (x <= 30):
            return 9  # "21_30"
        else:
            return 10  # ">30"

    data_agg = data_agg.with_columns(
        pl.col("sess_aid_dcount")
        .apply(lambda x: binning_aid_dcount(x))
        .alias("sess_binned_aid_dcount")
    )

    # drop cols
    data_agg = data_agg.drop(
        columns=[
            "curr_ts",
            "sess_aid_dcount",
            "sess_avg_hour_cart",
            "sess_avg_weekday_cart",
            "sess_click_count",
        ]
    )

    data_agg = freemem(data_agg)
    return data_agg
