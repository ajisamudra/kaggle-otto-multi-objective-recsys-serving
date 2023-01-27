import gc

import polars as pl


def freemem(df: pl.DataFrame):
    for col in df.columns:
        if df[col].dtype == pl.Int64:
            df = df.with_column(pl.col(col).cast(pl.Int32))
        elif df[col].dtype == pl.Float64:
            df = df.with_column(pl.col(col).cast(pl.Float32))
    gc.collect()
    return df


def round_float_3decimals(df: pl.DataFrame):
    for col in df.columns:
        if df[col].dtype in [pl.Float64, pl.Float32]:
            df = df.with_column(pl.col(col).apply(lambda x: round(x, 3)))
    gc.collect()
    return df


def calc_relative_diff_w_mean(df: pl.DataFrame, feature: str) -> pl.DataFrame:
    df = df.with_columns(
        [
            pl.col(f"{feature}").mean().over("session").alias(f"mean_{feature}"),
        ]
    )

    # difference with mean
    df = df.with_columns(
        [
            (pl.col(f"{feature}") - pl.col(f"mean_{feature}")).alias(
                f"diff_w_mean_{feature}"
            ),
        ]
    )

    # relative difference with mean
    df = df.with_columns(
        [
            (pl.col(f"diff_w_mean_{feature}") / pl.col(f"mean_{feature}")).alias(
                f"relative_diff_w_mean_{feature}"
            ),
        ]
    )

    # drop unused cols
    df = df.drop(
        columns=[
            f"mean_{feature}",
        ]
    )

    return df
