import polars as pl

from app.preprocess.utils import freemem


def make_session_representation_items(
    sess_df: pl.DataFrame,
):
    """
    df input
    session | type | ts | aid
    123 | 0 | 12313 | AID1
    123 | 1 | 12314 | AID1
    123 | 2 | 12345 | AID1
    """

    # agg per session & aid
    data_agg = sess_df.groupby(["session", "aid"]).agg(
        [
            pl.col("action_num_reverse_chrono")
            .min()
            .alias("min_action_num_reverse_chrono"),
            pl.col("log_recency_score").sum().alias("sum_log_recency_score"),
            pl.col("type_weighted_log_recency_score")
            .sum()
            .alias("sum_type_weighted_log_recency_score"),
            pl.col("duration_second").sum().alias("sum_duration_second"),
        ]
    )
    data_agg = data_agg.sort(pl.col("session"))

    # get last item per session
    data_aids = data_agg.filter(
        pl.col("min_action_num_reverse_chrono")
        == pl.col("min_action_num_reverse_chrono").min().over("session")
    ).select(["session", pl.col("aid").alias("last_event_in_session_aid")])

    # aid with max_log_recency_score
    data_aids_recency = data_agg.filter(
        pl.col("sum_log_recency_score")
        == pl.col("sum_log_recency_score").max().over("session")
    ).select(["session", pl.col("aid").alias("max_recency_event_in_session_aid")])

    # aid with max_type_weighted_log_recency_score
    data_aids_weighted_recency = data_agg.filter(
        pl.col("sum_type_weighted_log_recency_score")
        == pl.col("sum_type_weighted_log_recency_score").max().over("session")
    ).select(
        ["session", pl.col("aid").alias("max_weighted_recency_event_in_session_aid")]
    )

    # aid with log_duration_second
    data_aids_log_duration = data_agg.filter(
        pl.col("sum_duration_second")
        == pl.col("sum_duration_second").max().over("session")
    ).select(["session", pl.col("aid").alias("max_duration_event_in_session_aid")])
    # remove duplicate
    data_aids_log_duration = data_aids_log_duration.groupby("session").agg(
        [
            pl.col("max_duration_event_in_session_aid")
            .max()
            .alias("max_duration_event_in_session_aid")
        ]
    )

    data_aids = data_aids.join(
        data_aids_recency,
        how="left",
        left_on=["session"],
        right_on=["session"],
    )
    data_aids = data_aids.join(
        data_aids_weighted_recency,
        how="left",
        left_on=["session"],
        right_on=["session"],
    )
    data_aids = data_aids.join(
        data_aids_log_duration,
        how="left",
        left_on=["session"],
        right_on=["session"],
    )

    data_aids = freemem(data_aids)
    return data_aids
