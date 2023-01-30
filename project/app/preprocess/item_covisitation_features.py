import logging

import numpy as np
import polars as pl

from app.api.crud import (
    retrieve_item_covisit_buy2buy_weights,
    retrieve_item_covisit_buys_weights,
    retrieve_item_covisit_click_weights,
)
from app.preprocess.utils import freemem

log = logging.getLogger("uvicorn")


async def get_item_covisit_features(
    cand_df: pl.DataFrame, sess_representation: pl.DataFrame
) -> pl.DataFrame:

    # prepare source & target aid
    data_agg = cand_df.select(["candidate_aid", "session"])
    sess_repr = sess_representation.select(
        [
            "last_event_in_session_aid",
            "max_recency_event_in_session_aid",
            "max_weighted_recency_event_in_session_aid",
            "max_duration_event_in_session_aid",
        ]
    )

    # cross join data_agg with sess_representation
    data_agg = data_agg.join(sess_repr, how="cross")

    # we will compare candidate_aid with several different aid for each covisit weight
    # WEIGHT 1: covisit click weight

    results = await retrieve_item_covisit_click_weights(
        candidate_aids=cand_df["candidate_aid"].to_list()
    )

    data = np.array(results)
    columns = ["aid_x", "aid_y", "wgt"]
    weights_df = pl.DataFrame(data=data, columns=columns)
    weights_df = weights_df.with_columns(
        [
            pl.col("aid_x").cast(pl.Int32),
            pl.col("aid_y").cast(pl.Int32),
            pl.col("wgt").cast(pl.Float32),
        ]
    )

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "last_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename({"wgt": "click_weight_with_last_event_in_session_aid"})

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "max_recency_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename(
        {"wgt": "click_weight_with_max_recency_event_in_session_aid"}
    )

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "max_weighted_recency_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename(
        {"wgt": "click_weight_with_max_weighted_recency_event_in_session_aid"}
    )

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "max_duration_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename(
        {"wgt": "click_weight_with_max_duration_event_in_session_aid"}
    )

    # WEIGHT 2: covisit buys weight
    results = await retrieve_item_covisit_buys_weights(
        candidate_aids=cand_df["candidate_aid"].to_list()
    )

    data = np.array(results)
    columns = ["aid_x", "aid_y", "wgt"]
    weights_df = pl.DataFrame(data=data, columns=columns)
    weights_df = weights_df.with_columns(
        [
            pl.col("aid_x").cast(pl.Int32),
            pl.col("aid_y").cast(pl.Int32),
            pl.col("wgt").cast(pl.Float32),
        ]
    )

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "last_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename({"wgt": "buys_weight_with_last_event_in_session_aid"})

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "max_recency_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename(
        {"wgt": "buys_weight_with_max_recency_event_in_session_aid"}
    )

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "max_weighted_recency_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename(
        {"wgt": "buys_weight_with_max_weighted_recency_event_in_session_aid"}
    )

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "max_duration_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename(
        {"wgt": "buys_weight_with_max_duration_event_in_session_aid"}
    )

    # WEIGHT 3: covisit buy2buy weight
    results = await retrieve_item_covisit_buy2buy_weights(
        candidate_aids=cand_df["candidate_aid"].to_list()
    )

    data = np.array(results)
    columns = ["aid_x", "aid_y", "wgt"]
    weights_df = pl.DataFrame(data=data, columns=columns)
    weights_df = weights_df.with_columns(
        [
            pl.col("aid_x").cast(pl.Int32),
            pl.col("aid_y").cast(pl.Int32),
            pl.col("wgt").cast(pl.Float32),
        ]
    )

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "last_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename({"wgt": "buy2buy_weight_with_last_event_in_session_aid"})

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "max_recency_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename(
        {"wgt": "buy2buy_weight_with_max_recency_event_in_session_aid"}
    )

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "max_weighted_recency_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename(
        {"wgt": "buy2buy_weight_with_max_weighted_recency_event_in_session_aid"}
    )

    data_agg = data_agg.join(
        weights_df,
        how="left",
        left_on=["candidate_aid", "max_duration_event_in_session_aid"],
        right_on=["aid_x", "aid_y"],
    )
    data_agg = data_agg.rename(
        {"wgt": "buy2buy_weight_with_max_duration_event_in_session_aid"}
    )

    # # dummy data
    # data = {
    #     "candidate_aid": [i for i in range(100)],
    #     "click_weight_with_last_event_in_session_aid": [0.13 for i in range(100)],
    #     "click_weight_with_max_recency_event_in_session_aid": [0.4 for i in range(100)],
    #     "click_weight_with_max_weighted_recency_event_in_session_aid": [
    #         0.13 for i in range(100)
    #     ],
    #     "click_weight_with_max_duration_event_in_session_aid": [
    #         0.13 for i in range(100)
    #     ],
    #     "buys_weight_with_last_event_in_session_aid": [0.13 for i in range(100)],
    #     "buys_weight_with_max_recency_event_in_session_aid": [0.13 for i in range(100)],
    #     "buys_weight_with_max_weighted_recency_event_in_session_aid": [
    #         0.13 for i in range(100)
    #     ],
    #     "buys_weight_with_max_duration_event_in_session_aid": [
    #         0.13 for i in range(100)
    #     ],
    #     "buy2buy_weight_with_last_event_in_session_aid": [0.13 for i in range(100)],
    #     "buy2buy_weight_with_max_recency_event_in_session_aid": [
    #         0.13 for i in range(100)
    #     ],
    #     "buy2buy_weight_with_max_weighted_recency_event_in_session_aid": [
    #         0.13 for i in range(100)
    #     ],
    #     "buy2buy_weight_with_max_duration_event_in_session_aid": [
    #         0.13 for i in range(100)
    #     ],
    # }

    # data_agg = pl.DataFrame(data)
    data_agg = freemem(data_agg)

    return data_agg
