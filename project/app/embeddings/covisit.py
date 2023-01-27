import polars as pl

from app.preprocess.utils import freemem

# load annoy index

# load keyed dict

# function to make word2vec features


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
