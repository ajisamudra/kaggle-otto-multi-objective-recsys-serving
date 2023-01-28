import polars as pl
from starlette.requests import Request

from app.data_models.pydantic import PayloadSchema
from app.preprocess.utils import freemem
from app.retrieval.covisit_retrieval import covisit_retrieval
from app.retrieval.past_aids_retrieval import past_aids_retrieval
from app.retrieval.word2vec_retrieval import word2vec_retrieval


def retrieve_candidates(payload: PayloadSchema, request: Request) -> pl.DataFrame:
    """
    Output scheme should be

    candidate_aid: int
    retrieval_covisit: int
    retrieval_word2vec: int
    rank_covisit: int
    rank_word2vec: int
    retrieval_combined: int
    rank_combined: int

    """
    # retrieve past aids
    candidates1, ranks1 = past_aids_retrieval(payload=payload)

    # retrieve covisit
    candidates2, ranks2 = covisit_retrieval(payload=payload)

    # retieve word2vec
    candidates3, ranks3 = word2vec_retrieval(payload=payload, request=request)

    # combine candidates & its ranks
    # first candidate
    cands = candidates1
    rank_past_aids = ranks1
    rank_covisit = [len(ranks2) for i in range(len(cands))]
    rank_word2vec = [len(ranks3) for i in range(len(cands))]

    # second candidate
    cands.extend(candidates2)
    rank_past_aids.extend([len(ranks1) for i in range(len(candidates2))])
    rank_covisit.extend(ranks2)
    rank_word2vec.extend([len(ranks3) for i in range(len(candidates2))])

    # third candidate
    cands.extend(candidates3)
    rank_past_aids.extend([len(ranks1) for i in range(len(candidates3))])
    rank_covisit.extend([len(ranks2) for i in range(len(candidates3))])
    rank_word2vec.extend(ranks3)

    # OHE retrieval
    is_covisit = [1 if c in candidates2 else 0 for c in cands]
    is_word2vec = [1 if c in candidates3 else 0 for c in cands]

    # create candidates_df
    data = {
        "candidate_aid": candidates1,
        "retrieval_covisit": is_covisit,
        "retrieval_word2vec": is_word2vec,
        "rank_past_aids": rank_past_aids,
        "rank_covisit": rank_covisit,
        "rank_word2vec": rank_word2vec,
    }
    candidates = pl.DataFrame(data)

    # remove duplicate
    candidates = candidates.groupby(["candidate_aid"]).agg(
        [
            pl.col("retrieval_covisit").max(),
            pl.col("retrieval_word2vec").max(),
            pl.col("rank_past_aids").min(),
            pl.col("rank_covisit").min(),
            pl.col("rank_word2vec").min(),
        ]
    )

    # create combined rank and ohe strategy
    candidates = candidates.with_columns(
        [
            (pl.col("retrieval_covisit") + pl.col("retrieval_word2vec")).alias(
                "retrieval_combined"
            ),
            pl.min(
                [
                    "rank_past_aids",
                    "rank_covisit",
                    "rank_word2vec",
                ]
            ).alias("rank_combined"),
        ]
    )

    # drop unecessary columns
    candidates = candidates.drop(
        columns=[
            "rank_past_aids",
        ]
    )

    # change 64 bit to 32 bit
    candidates = freemem(candidates)

    return candidates
