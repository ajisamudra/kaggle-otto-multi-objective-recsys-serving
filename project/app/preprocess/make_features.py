import numpy as np
import polars as pl

from app.data_models.pydantic import PayloadSchema
from app.preprocess.item_features import get_item_features
from app.preprocess.item_hour_features import get_item_hour_features
from app.preprocess.item_weekday_features import get_item_weekday_features
from app.preprocess.preprocess_events import create_session_df
from app.preprocess.session_features import make_session_features
from app.preprocess.session_item_features import make_session_item_features


def make_features(payload: PayloadSchema, candidates: pl.DataFrame) -> pl.DataFrame:
    # create session
    sess_df = create_session_df(payload=payload)

    # FEATURE 1: session features
    sess_fea_df = make_session_features(data=sess_df)

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
            pl.Series(2**linear_interpolation - 1)
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
    print(f"join sess_fea shape : {candidates.shape}")

    # create session representation

    # FEATURE 2: covisit weight features

    # item-covisit features
    # get features with candidate_aid & session representation

    # FEATURE 3: word2vec distance features

    # item-word2vec features
    # get features with candidate_aid & last_aid in session

    # FEATURE 4: session-item features
    # session-item features
    sess_item_fea_df = make_session_item_features(data=sess_df)

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
    print(f"join sess_item_fea shape : {candidates.shape}")

    # FEATURE 5: item features
    item_fea_df = get_item_features(cand_df=candidates)
    candidates = candidates.join(
        item_fea_df,
        how="left",
        left_on=["candidate_aid"],
        right_on=["aid"],
    )

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

    print(f"join item_fea shape : {candidates.shape}")

    # FEATURE 6: item-hour features

    # item-hour features
    # get features with candidate_aid & sess_hour
    item_hour_fea_df = get_item_hour_features(
        cand_df=candidates, hour=sess_fea_df["sess_hour"]
    )
    candidates = candidates.join(
        item_hour_fea_df,
        how="left",
        left_on=["candidate_aid", "sess_hour"],
        right_on=["aid", "hour"],
    ).fill_null(0)

    print(f"join item_hour_fea shape : {candidates.shape}")

    # FEATURE 7: item-weekday features

    # item-weekday features
    # get features with candidate_aid & sess_weekday
    item_weekday_fea_df = get_item_weekday_features(
        cand_df=candidates, weekday=sess_fea_df["sess_weekday"]
    )
    candidates = candidates.join(
        item_weekday_fea_df,
        how="left",
        left_on=["candidate_aid", "sess_weekday"],
        right_on=["aid", "weekday"],
    ).fill_null(0)

    print(f"join item_weekday_fea shape : {candidates.shape}")

    # drop cols
    candidates = candidates.drop(
        columns=[
            "session",
            "sess_hour",
            "sess_weekday",
            # "buy2buy_weight_min",
            # "click_weight_min",
        ]
    )

    print(f"final dataset : {candidates.shape}")

    print(candidates["candidate_aid"].to_list())
    print(candidates.columns)
    return candidates
