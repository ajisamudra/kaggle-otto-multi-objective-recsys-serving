import logging
import time

import polars as pl
from starlette.requests import Request

from app.data_models.pydantic import PayloadSchema
from app.preprocess.utils import freemem
from app.retrieval.covisit_retrieval import covisit_retrieval
from app.retrieval.word2vec_retrieval import word2vec_retrieval

log = logging.getLogger("uvicorn")


async def retrieve_candidates(payload: PayloadSchema, request: Request) -> pl.DataFrame:
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
    # retrieve covisit
    # including retrieve past aids
    time1 = time.time()
    candidates1, ranks1 = await covisit_retrieval(payload=payload, request=request)
    time2 = time.time()

    # retieve word2vec
    candidates2, ranks2 = word2vec_retrieval(payload=payload, request=request)
    time3 = time.time()

    # combine candidates & its ranks
    # first candidate
    cands = candidates1
    rank_covisit = ranks1
    rank_word2vec = [len(ranks2) for i in range(len(candidates1))]

    # second candidate
    cands.extend(candidates2)
    rank_covisit.extend([len(ranks1) for i in range(len(candidates2))])
    rank_word2vec.extend(ranks2)

    # OHE retrieval
    is_covisit = [1 if c in candidates1 else 0 for c in cands]
    is_word2vec = [1 if c in candidates2 else 0 for c in cands]

    # create candidates_df
    data = {
        "candidate_aid": cands,
        "retrieval_covisit": is_covisit,
        "retrieval_word2vec": is_word2vec,
        "rank_covisit": rank_covisit,
        "rank_word2vec": rank_word2vec,
    }
    candidates = pl.DataFrame(data)

    # remove duplicate
    candidates = candidates.groupby(["candidate_aid"]).agg(
        [
            pl.col("retrieval_covisit").max(),
            pl.col("retrieval_word2vec").max(),
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
                    "rank_covisit",
                    "rank_word2vec",
                ]
            ).alias("rank_combined"),
        ]
    )

    # change 64 bit to 32 bit
    candidates = freemem(candidates)
    time4 = time.time()

    # log time taken
    time21 = time2 - time1
    time32 = time3 - time2
    time43 = time4 - time3
    time41 = time4 - time1
    log.info("================ retrieval ================")
    log.info(f"time taken covisit + word2vec + post-retrieval: {round(time41,4)} s")
    log.info(f"time taken covisit retrieval = {round(time21,4)} s")
    log.info(f"time taken word2vec retrieval = {round(time32,4)} s")
    log.info(f"time taken post-retrieval = {round(time43,4)} s")
    log.info("================ retrieval ================")

    return candidates
