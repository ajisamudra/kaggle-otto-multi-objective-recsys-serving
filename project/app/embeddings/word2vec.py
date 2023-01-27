import polars as pl

from app.preprocess.utils import freemem

# load annoy index

# load keyed dict

# function to make word2vec features


# input candidate_aid, last_event
def make_item_word2vec_features(cand_df: pl.DataFrame, last_event: pl.Series):

    # dummy data
    data = {
        "candidate_aid": [i for i in range(100)],
        "word2vec_skipgram_last_event_cosine_distance": [0.13 for i in range(100)],
        "word2vec_skipgram_last_event_euclidean_distance": [0.4 for i in range(100)],
    }

    data_agg = pl.DataFrame(data)
    data_agg = freemem(data_agg)

    return data_agg
