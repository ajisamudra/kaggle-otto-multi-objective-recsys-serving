import numpy as np
import polars as pl
from annoy import AnnoyIndex
from gensim.models import KeyedVectors
from starlette.requests import Request

from app.preprocess.utils import freemem, round_float_3decimals
from utils import constants

NTREE = 15
VECT_SIZE = 32

############
# Load the Annoy Index
############
# load keyed vectors
def load_word2vec_vect32_wdw3_embedding():
    # load keyed vectors
    filepath = constants.WORD2VEC_PATH
    kvectors = KeyedVectors.load(filepath, mmap="r")
    return kvectors


# load annoy index
def load_annoy_idx_word2vec_vect32_wdw3_embedding():

    # load keyed vectors
    kvectors = load_word2vec_vect32_wdw3_embedding()

    # create annoy index for search nn
    aid2idx = {aid: i for i, aid in enumerate(kvectors.index_to_key)}
    index = AnnoyIndex(VECT_SIZE, "angular")

    for aid, idx in aid2idx.items():
        index.add_item(aid, kvectors.vectors[idx])

    # build annoy index
    index.build(NTREE)

    return index


############
# function to make word2vec features
############


def vectorized_cosine_distance(vectors1: np.ndarray, vectors2: np.ndarray):
    """
    Reference: https://www.geeksforgeeks.org/how-to-calculate-cosine-similarity-in-python/
    """
    cosine_distances = np.sum(vectors1 * vectors2, axis=1) / (
        np.linalg.norm(vectors1, axis=1) * np.linalg.norm(vectors2, axis=1)
    )
    return cosine_distances


def vectorized_euclidean_distance(vectors1: np.ndarray, vectors2: np.ndarray):
    return np.linalg.norm((vectors1 - vectors2), axis=1)


def calculate_distance_metrics(
    embedding: AnnoyIndex,
    candidate_aids: list,
    last_event_aid: int,
):
    vectors1 = []
    vectors2 = []

    # get vectors of candidates
    for source in candidate_aids:
        # vector1 = embedding.get_vector(source)
        try:
            vector1 = embedding.get_item_vector(source)
        except KeyError:
            vector1 = [0 for _ in range(VECT_SIZE)]

        vectors1.append(vector1)

    # get vectors of last event aid
    try:
        vector2 = embedding.get_item_vector(last_event_aid)
    except KeyError:
        vector2 = [0 for _ in range(VECT_SIZE)]
    vectors2 = [vector2 for i in range(len(candidate_aids))]

    # convert list to array 2d
    nd_vectors1 = np.array(vectors1)
    nd_vectors2 = np.array(vectors2)

    # compute cosine similarity
    last_event_cosine_distances = vectorized_cosine_distance(nd_vectors1, nd_vectors2)
    last_event_euclidean_distances = vectorized_euclidean_distance(
        nd_vectors1, nd_vectors2
    )

    return (
        last_event_cosine_distances,
        last_event_euclidean_distances,
    )


# input candidate_aid, last_event
def make_item_word2vec_features(
    request: Request, candidate_aids: list, last_event: int
):

    # (
    #     word2vec_skipgram_last_event_cosine_distances,
    #     word2vec_skipgram_last_event_euclidean_distances,
    # ) = calculate_distance_metrics(
    #     embedding=request.app.state.word2vec_idx,
    #     candidate_aids=candidate_aids,
    #     last_event_aid=last_event,
    # )

    # # save word2vec distance features
    # output_data = {
    #     "candidate_aid": candidate_aids,
    #     "word2vec_skipgram_last_event_cosine_distance": word2vec_skipgram_last_event_cosine_distances,
    #     "word2vec_skipgram_last_event_euclidean_distance": word2vec_skipgram_last_event_euclidean_distances,
    # }

    # data_agg = pl.DataFrame(output_data)
    # data_agg = freemem(data_agg)
    # data_agg = round_float_3decimals(data_agg)

    # dummy data
    data = {
        "candidate_aid": [i for i in range(100)],
        "word2vec_skipgram_last_event_cosine_distance": [0.13 for i in range(100)],
        "word2vec_skipgram_last_event_euclidean_distance": [0.4 for i in range(100)],
    }

    data_agg = pl.DataFrame(data)
    data_agg = freemem(data_agg)

    return data_agg


############
# function to make word2vec candidates
############


def suggest_clicks_word2vec(
    n_candidate: int,
    aids: list,
    embedding: AnnoyIndex,
):

    # unique_aids = set(aids)
    unique_aids = list(dict.fromkeys(aids[::-1]))
    candidates = embedding.get_nns_by_item(unique_aids[0], n=n_candidate + 1)
    # drop first result which is the query aid
    candidates = candidates[1:]

    # append to list result
    rank_list = [i for i in range(n_candidate)]

    return candidates, rank_list
