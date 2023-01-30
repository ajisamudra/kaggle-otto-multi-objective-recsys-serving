import logging
import time

import polars as pl
from starlette.requests import Request

from app.data_models.pydantic import PayloadSchema
from app.preprocess.make_features import make_features
from app.retrieval.all_retrieval import retrieve_candidates

log = logging.getLogger("uvicorn")


async def retrieve_and_make_features(
    request: Request, payload: PayloadSchema
) -> pl.DataFrame:
    # retrieve candidates
    time1 = time.time()
    candidates = await retrieve_candidates(payload=payload, request=request)
    time2 = time.time()

    # make features for the candidates
    df_features = await make_features(
        candidates=candidates, payload=payload, request=request
    )
    time3 = time.time()

    # log time taken
    time21 = time2 - time1
    time32 = time3 - time2
    time31 = time3 - time1
    log.info("================ preprocess ================")
    log.info(f"time taken retrieval + make features: {round(time31,4)} s")
    log.info(f"time taken retrieval = {round(time21,4)} s")
    log.info(f"time taken make features = {round(time32,4)} s")
    log.info("================ preprocess ================")

    return df_features
