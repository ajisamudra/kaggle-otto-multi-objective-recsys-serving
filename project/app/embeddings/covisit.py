import itertools
from collections import Counter

import numpy as np
import pandas as pd
import polars as pl

from app.preprocess.utils import freemem
from utils import constants

############
# Load Covisit Matrix as dict
############

DISK_PIECES = 4
VER_FEA = 5


def pqt_to_dict(df):
    return df.groupby("aid_x").aid_y.apply(list).to_dict()


def load_top15_covisitation_buys():
    covisit_dir = constants.COVISIT_PATH
    top_15_buys = pqt_to_dict(
        pd.read_parquet(f"{covisit_dir}/top_15_carts_orders_v{VER_FEA}_0.pqt")
    )

    return top_15_buys


def load_top15_covisitation_buy2buy():
    covisit_dir = constants.COVISIT_PATH
    top_15_buy2buy = pqt_to_dict(
        pd.read_parquet(f"{covisit_dir}/top_15_buy2buy_v{VER_FEA}_0.pqt")
    )
    return top_15_buy2buy


############
# Covisit Retrieval
############


def suggest_candidates_covisit(
    n_past_aids_cand: int,
    n_covisit_cand: int,
    aids: list,
    event_types: list,
    covisit_buys: dict,
    covisit_buy2buy: dict,
):
    type_weight_multipliers = {0: 0.5, 1: 9, 2: 0.5}
    unique_buys = []
    unique_aids = list(dict.fromkeys(aids[::-1]))
    types = event_types
    for ix, aid in enumerate(aids):
        curr_type = types[ix]
        if (curr_type == 1) or (curr_type == 2):
            unique_buys.append(aid)

    # reverse the order
    unique_buys = list(dict.fromkeys(unique_buys[::-1]))

    # past aids
    # RERANK CANDIDATES USING WEIGHTS
    if len(unique_aids) > 20:
        weights = np.logspace(0.1, 1, len(aids), base=2, endpoint=True) - 1
        aids_temp = Counter()
        # RERANK BASED ON REPEAT ITEMS AND TYPE OF ITEMS
        for aid, w, t in zip(aids, weights, types):
            aids_temp[aid] += w * type_weight_multipliers[t]
        past_aid_candidate = [k for k, v in aids_temp.most_common(n_past_aids_cand)]

    else:
        past_aid_candidate = list(unique_aids)[:n_past_aids_cand]

    # covisit candidate
    # RERANK CANDIDATES USING WEIGHTS
    if len(unique_aids) >= 20:
        # USE "CART ORDER" CO-VISITATION MATRIX
        aids2 = list(
            itertools.chain(
                *[covisit_buys[aid] for aid in unique_aids if aid in covisit_buys]
            )
        )

        # RERANK CANDIDATES USING "BUY2BUY" CO-VISITATION MATRIX
        aids3 = list(
            itertools.chain(
                *[covisit_buy2buy[aid] for aid in unique_buys if aid in covisit_buy2buy]
            )
        )

        # RERANK CANDIDATES
        top_aids2 = [
            aid2
            for aid2, cnt in Counter(aids2 + aids3).most_common(n_covisit_cand)
            if aid2 not in unique_aids
        ]
        covisit_candidate = top_aids2[:n_covisit_cand]

    else:
        # USE "CART ORDER" CO-VISITATION MATRIX
        aids2 = list(
            itertools.chain(
                *[covisit_buys[aid] for aid in unique_aids if aid in covisit_buys]
            )
        )
        # USE "BUY2BUY" CO-VISITATION MATRIX
        aids3 = list(
            itertools.chain(
                *[covisit_buy2buy[aid] for aid in unique_buys if aid in covisit_buy2buy]
            )
        )
        # RERANK CANDIDATES
        top_aids2 = [
            aid2
            for aid2, cnt in Counter(aids2 + aids3).most_common(n_covisit_cand)
            if aid2 not in unique_aids
        ]
        covisit_candidate = top_aids2[:n_covisit_cand]

    # final candidates
    candidates = past_aid_candidate + covisit_candidate
    ranks = [i for i in range(len(candidates))]

    return candidates, ranks


############
# function to make covisit features
# will use postgres to store the data instead
############

# input candidate_aid, session representation
def make_item_covisit_features(
    cand_df: pl.DataFrame, sess_representation: pl.DataFrame
):

    # dummy data
    data = {
        "candidate_aid": [i for i in range(100)],
        "click_weight_with_last_event_in_session_aid": [0.13 for i in range(100)],
        "click_weight_with_max_recency_event_in_session_aid": [0.4 for i in range(100)],
        "click_weight_with_max_weighted_recency_event_in_session_aid": [
            0.13 for i in range(100)
        ],
        "click_weight_with_max_duration_event_in_session_aid": [
            0.13 for i in range(100)
        ],
        "buys_weight_with_last_event_in_session_aid": [0.13 for i in range(100)],
        "buys_weight_with_max_recency_event_in_session_aid": [0.13 for i in range(100)],
        "buys_weight_with_max_weighted_recency_event_in_session_aid": [
            0.13 for i in range(100)
        ],
        "buys_weight_with_max_duration_event_in_session_aid": [
            0.13 for i in range(100)
        ],
        "buy2buy_weight_with_last_event_in_session_aid": [0.13 for i in range(100)],
        "buy2buy_weight_with_max_recency_event_in_session_aid": [
            0.13 for i in range(100)
        ],
        "buy2buy_weight_with_max_weighted_recency_event_in_session_aid": [
            0.13 for i in range(100)
        ],
        "buy2buy_weight_with_max_duration_event_in_session_aid": [
            0.13 for i in range(100)
        ],
    }

    data_agg = pl.DataFrame(data)
    data_agg = freemem(data_agg)

    return data_agg
