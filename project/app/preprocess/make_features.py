import logging
import time

import numpy as np
import polars as pl
from starlette.requests import Request

from app.data_models.pydantic import PayloadSchema
from app.embeddings.word2vec import make_item_word2vec_features
from app.preprocess.item_covisitation_features import get_item_covisit_features
from app.preprocess.item_features import get_item_features
from app.preprocess.item_hour_features import get_item_hour_features
from app.preprocess.item_weekday_features import get_item_weekday_features
from app.preprocess.preprocess_events import create_session_df
from app.preprocess.session_features import make_session_features
from app.preprocess.session_item_features import make_session_item_features
from app.preprocess.session_representation_items import (
    make_session_representation_items,
)
from app.preprocess.utils import calc_relative_diff_w_mean

log = logging.getLogger("uvicorn")


async def make_features(
    payload: PayloadSchema, candidates: pl.DataFrame, request: Request
) -> pl.DataFrame:
    # create session
    time1 = time.time()
    sess_df = create_session_df(payload=payload)

    # FEATURE 1: session features
    time2 = time.time()
    sess_fea_df = make_session_features(sess_df=sess_df)

    # join candidate with sess_fea_df -> get sess_hour & sess_weekday
    candidates = candidates.join(sess_fea_df, how="cross")

    # additional after join with sess features
    candidates = candidates.with_columns(
        [
            pl.col("rank_covisit").max().over("session").alias("max_rank_covisit"),
        ]
    )
    # add log_rank_covisit_score
    linear_interpolation = 0.1 + ((1 - 0.1) / (candidates["max_rank_covisit"] - 1)) * (
        candidates["max_rank_covisit"] - candidates["rank_covisit"]
    )
    candidates = candidates.with_columns(
        [
            pl.Series(2 ** linear_interpolation - 1)
            .alias("log_rank_covisit_score")
            .fill_nan(1),
        ]
    )
    candidates = candidates.fill_null(0)

    # fraction of rank score to total rank score in session
    candidates = candidates.with_columns(
        [
            # window event count session
            pl.col("log_rank_covisit_score")
            .sum()
            .over("session")
            .alias("total_log_rank_covisit_score"),
        ],
    )

    candidates = candidates.with_columns(
        [
            # frac compare to total in particular session
            (pl.col("log_rank_covisit_score") / pl.col("total_log_rank_covisit_score"))
            .fill_nan(0)
            .alias("frac_log_rank_covisit_score_to_all"),
        ],
    )

    # drop cols
    candidates = candidates.drop(
        columns=["max_rank_covisit", "total_log_rank_covisit_score"]
    )
    log.info(f"join sess_fea shape : {candidates.shape}")

    # create session representation
    time3 = time.time()
    sess_repr_df = make_session_representation_items(sess_df=sess_df)

    # FEATURE 2: covisit weight features

    # item-covisit features
    # get features with candidate_aid & session representation
    time4 = time.time()
    item_covisit_fea_df = await get_item_covisit_features(
        cand_df=candidates,
        sess_representation=sess_repr_df,
    )
    candidates = candidates.join(
        item_covisit_fea_df,
        how="left",
        left_on=["candidate_aid"],
        right_on=["candidate_aid"],
    )
    candidates = candidates.fill_null(0)

    # additional transformation
    candidates = candidates.with_columns(
        [
            np.mean(
                [
                    pl.col("click_weight_with_last_event_in_session_aid"),
                    pl.col("click_weight_with_max_recency_event_in_session_aid"),
                    pl.col(
                        "click_weight_with_max_weighted_recency_event_in_session_aid"
                    ),
                    pl.col("click_weight_with_max_duration_event_in_session_aid"),
                ]
            ).alias("click_weight_mean"),
            pl.max(
                [
                    pl.col("click_weight_with_last_event_in_session_aid"),
                    pl.col("click_weight_with_max_recency_event_in_session_aid"),
                    pl.col(
                        "click_weight_with_max_weighted_recency_event_in_session_aid"
                    ),
                    pl.col("click_weight_with_max_duration_event_in_session_aid"),
                ]
            ).alias("click_weight_max"),
            pl.min(
                [
                    pl.col("click_weight_with_last_event_in_session_aid"),
                    pl.col("click_weight_with_max_recency_event_in_session_aid"),
                    pl.col(
                        "click_weight_with_max_weighted_recency_event_in_session_aid"
                    ),
                    pl.col("click_weight_with_max_duration_event_in_session_aid"),
                ]
            ).alias("click_weight_min"),
            np.mean(
                [
                    pl.col("buys_weight_with_last_event_in_session_aid"),
                    pl.col("buys_weight_with_max_recency_event_in_session_aid"),
                    pl.col(
                        "buys_weight_with_max_weighted_recency_event_in_session_aid"
                    ),
                    pl.col("buys_weight_with_max_duration_event_in_session_aid"),
                ]
            ).alias("buys_weight_mean"),
            pl.max(
                [
                    pl.col("buys_weight_with_last_event_in_session_aid"),
                    pl.col("buys_weight_with_max_recency_event_in_session_aid"),
                    pl.col(
                        "buys_weight_with_max_weighted_recency_event_in_session_aid"
                    ),
                    pl.col("buys_weight_with_max_duration_event_in_session_aid"),
                ]
            ).alias("buys_weight_max"),
            pl.min(
                [
                    pl.col("buys_weight_with_last_event_in_session_aid"),
                    pl.col("buys_weight_with_max_recency_event_in_session_aid"),
                    pl.col(
                        "buys_weight_with_max_weighted_recency_event_in_session_aid"
                    ),
                    pl.col("buys_weight_with_max_duration_event_in_session_aid"),
                ]
            ).alias("buys_weight_min"),
            np.mean(
                [
                    pl.col("buy2buy_weight_with_last_event_in_session_aid"),
                    pl.col("buy2buy_weight_with_max_recency_event_in_session_aid"),
                    pl.col(
                        "buy2buy_weight_with_max_weighted_recency_event_in_session_aid"
                    ),
                    pl.col("buy2buy_weight_with_max_duration_event_in_session_aid"),
                ]
            ).alias("buy2buy_weight_mean"),
            pl.max(
                [
                    pl.col("buy2buy_weight_with_last_event_in_session_aid"),
                    pl.col("buy2buy_weight_with_max_recency_event_in_session_aid"),
                    pl.col(
                        "buy2buy_weight_with_max_weighted_recency_event_in_session_aid"
                    ),
                    pl.col("buy2buy_weight_with_max_duration_event_in_session_aid"),
                ]
            ).alias("buy2buy_weight_max"),
            pl.min(
                [
                    pl.col("buy2buy_weight_with_last_event_in_session_aid"),
                    pl.col("buy2buy_weight_with_max_recency_event_in_session_aid"),
                    pl.col(
                        "buy2buy_weight_with_max_weighted_recency_event_in_session_aid"
                    ),
                    pl.col("buy2buy_weight_with_max_duration_event_in_session_aid"),
                ]
            ).alias("buy2buy_weight_min"),
        ]
    )

    candidates = candidates.with_columns(
        [
            (pl.col("buy2buy_weight_max") - pl.col("buy2buy_weight_min")).alias(
                "buy2buy_weight_diff_max_min"
            ),
            (pl.col("buys_weight_max") - pl.col("buys_weight_min")).alias(
                "buys_weight_diff_max_min"
            ),
            (pl.col("click_weight_max") - pl.col("click_weight_min")).alias(
                "click_weight_diff_max_min"
            ),
        ]
    )

    log.info(f"join covisit_fea shape : {candidates.shape}")

    # FEATURE 3: word2vec distance features

    # item-word2vec features
    # get features with candidate_aid & last_aid in session
    time5 = time.time()
    item_word2vec_fea_df = make_item_word2vec_features(
        request=request,
        candidate_aids=candidates["candidate_aid"].to_list(),
        last_event=sess_repr_df["last_event_in_session_aid"].to_list()[0],
    )
    candidates = candidates.join(
        item_word2vec_fea_df,
        how="left",
        left_on=["candidate_aid"],
        right_on=["candidate_aid"],
    )
    log.info(f"join word2vec_fea shape : {candidates.shape}")

    # FEATURE 4: session-item features
    # session-item features
    time6 = time.time()
    sess_item_fea_df = make_session_item_features(sess_df=sess_df)

    # join candidate with sess_item_fea_df
    candidates = candidates.join(
        sess_item_fea_df,
        how="left",
        left_on=["session", "candidate_aid"],
        right_on=["session", "aid"],
    )

    # additonal transformation after join
    # get ratio of sesXaid_mins_from_last_event with sess_durations
    candidates = candidates.with_columns(
        [
            (pl.col("sesXaid_mins_from_last_event") / pl.col("sess_duration_mins"))
            .fill_null(99999)
            .alias("sesXaid_frac_mins_from_last_event_to_sess_duration")
        ]
    )

    candidates = candidates.with_columns(
        [
            # the higher the better, fill_null with 0
            pl.col("sesXaid_type_weighted_log_recency_score")
            .fill_null(0)
            .alias("sesXaid_type_weighted_log_recency_score"),
            pl.col("sesXaid_log_recency_score")
            .fill_null(0)
            .alias("sesXaid_log_recency_score"),
            # pl.col("sesXaid_events_count").fill_null(0).alias("sesXaid_events_count"),
            pl.col("sesXaid_click_count").fill_null(0).alias("sesXaid_click_count"),
            pl.col("sesXaid_cart_count").fill_null(0).alias("sesXaid_cart_count"),
            # pl.col("sesXaid_order_count").fill_null(0).alias("sesXaid_order_count"),
            pl.col("sesXaid_avg_click_dur_sec")
            .fill_null(0)
            .alias("sesXaid_avg_click_dur_sec"),
            pl.col("sesXaid_avg_cart_dur_sec")
            .fill_null(0)
            .alias("sesXaid_avg_cart_dur_sec"),
            pl.col("sesXaid_type_dcount").fill_null(0).alias("sesXaid_type_dcount"),
            # the lower the better, fill_null with high number
            pl.col("sesXaid_action_num_reverse_chrono")
            .fill_null(500)
            .alias("sesXaid_action_num_reverse_chrono"),
            pl.col("sesXaid_mins_from_last_event")
            .fill_null(9999.0)
            .alias("sesXaid_mins_from_last_event"),
            pl.col("sesXaid_mins_from_last_event_log1p")
            .fill_null(99.0)
            .alias("sesXaid_mins_from_last_event_log1p"),
        ]
    )
    candidates = candidates.fill_null(0)
    log.info(f"join sess_item_fea shape : {candidates.shape}")

    # FEATURES 4-B: relative distance features
    time7 = time.time()
    DIF_FEAS = [
        "click_weight_with_last_event_in_session_aid",
        "buys_weight_with_last_event_in_session_aid",
        "buy2buy_weight_with_last_event_in_session_aid",
        "word2vec_skipgram_last_event_cosine_distance",
        "word2vec_skipgram_last_event_euclidean_distance",
    ]
    for f in DIF_FEAS:
        candidates = calc_relative_diff_w_mean(df=candidates, feature=f)
    # fill 0
    candidates = candidates.fill_nan(0)
    log.info(f"join relative_distance_fea shape : {candidates.shape}")

    # FEATURE 5: item features
    time8 = time.time()
    item_fea_df = await get_item_features(
        candidate_aids=candidates["candidate_aid"].to_list()
    )
    candidates = candidates.join(
        item_fea_df,
        how="left",
        left_on=["candidate_aid"],
        right_on=["aid"],
    ).fill_nan(0)

    candidates = candidates.with_columns(
        [
            np.abs(pl.col("sess_hour") - pl.col("item_avg_hour_order"))
            .cast(pl.Int32)
            .fill_null(99)
            .alias("sessXitem_abs_diff_avg_hour_order"),
            np.abs(pl.col("sess_weekday") - pl.col("item_avg_weekday_order"))
            .cast(pl.Int32)
            .fill_null(99)
            .alias("sessXitem_abs_diff_avg_weekday_order"),
        ]
    )
    candidates = candidates.fill_null(0)

    log.info(f"join item_fea shape : {candidates.shape}")

    # FEATURE 6: item-hour features
    time9 = time.time()

    # item-hour features
    # get features with candidate_aid & sess_hour
    item_hour_fea_df = await get_item_hour_features(
        candidate_aids=candidates["candidate_aid"].to_list(),
        hours=sess_fea_df["sess_hour"].to_list(),
    )
    candidates = candidates.join(
        item_hour_fea_df,
        how="left",
        left_on=["candidate_aid", "sess_hour"],
        right_on=["aid", "hour"],
    ).fill_nan(0)

    log.info(f"join item_hour_fea shape : {candidates.shape}")

    # FEATURE 7: item-weekday features
    time10 = time.time()

    # item-weekday features
    # get features with candidate_aid & sess_weekday
    item_weekday_fea_df = await get_item_weekday_features(
        candidate_aids=candidates["candidate_aid"].to_list(),
        weekdays=sess_fea_df["sess_weekday"].to_list(),
    )
    candidates = candidates.join(
        item_weekday_fea_df,
        how="left",
        left_on=["candidate_aid", "sess_weekday"],
        right_on=["aid", "weekday"],
    ).fill_nan(0)

    log.info(f"join item_weekday_fea shape : {candidates.shape}")

    # drop cols
    candidates = candidates.drop(
        columns=[
            "session",
            "sess_hour",
            "sess_weekday",
            "buy2buy_weight_min",
            "click_weight_min",
        ]
    )

    # log.info(candidates.columns)
    log.info(f"final dataset : {candidates.shape}")
    time11 = time.time()

    # log time taken
    time_item_weekday = time11 - time10
    time_item_hour = time10 - time9
    time_item = time9 - time8
    time_sess_item = time7 - time6
    time_word2vec = time6 - time5
    time_covisit = time5 - time4
    time_sess_represent = time4 - time3
    time_sess_fea = time3 - time2
    time_total = time11 - time1

    log.info("================ make features ================")
    log.info(f"time taken all features: {round(time_total,4)} s")
    log.info(f"time taken session features = {round(time_sess_fea,4)} s")
    log.info(f"time taken session representation = {round(time_sess_represent,4)} s")
    log.info(f"time taken covisit weight features = {round(time_covisit,4)} s")
    log.info(f"time taken word2vec distance features = {round(time_word2vec,4)} s")
    log.info(f"time taken session-item features = {round(time_sess_item,4)} s")
    log.info(f"time taken item features = {round(time_item,4)} s")
    log.info(f"time taken item-hour features = {round(time_item_hour,4)} s")
    log.info(f"time taken item-weekday features = {round(time_item_weekday,4)} s")
    log.info("================ make features ================")

    return candidates
