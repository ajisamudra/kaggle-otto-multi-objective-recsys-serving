import logging
import time

import polars as pl
from fastapi import APIRouter, Depends
from starlette.requests import Request

from app.data_models.pydantic import PayloadSchema, ResponseSchema
from app.preprocess.retrieve_and_make_features import retrieve_and_make_features

router = APIRouter()

log = logging.getLogger("uvicorn")


@router.post(
    "/predict",
    response_model=ResponseSchema,
)
async def predict(request: Request, payload: PayloadSchema):

    # retrieve the ranker_ml object from startup
    ranker_ml = request.app.state.ranker_ml

    # do retrieval candidates & enrich features for these candidates
    time1 = time.time()
    df_features = await retrieve_and_make_features(request=request, payload=payload)

    # select features
    time2 = time.time()
    selected_features = df_features.columns
    selected_features.remove("candidate_aid")
    X_test = df_features[selected_features].to_pandas()
    df_features = df_features.select(["candidate_aid"])

    # scoring
    scores = ranker_ml.model.predict(X_test)
    df_features = df_features.with_columns([pl.Series(name="score", values=scores)])

    # sort candidates aid based on some metrics
    metrics = "score"
    df_features = df_features.sort([metrics], reverse=True).head(20)
    time3 = time.time()

    # log time taken
    time21 = time2 - time1
    time32 = time3 - time2
    time31 = time3 - time1
    log.info("================ preprocess + scoring ================")
    log.info(f"time taken preprocess + scoring: {round(time31,4)} s")
    log.info(f"time taken preprocess = {round(time21,4)} s")
    log.info(f"time taken scoring = {round(time32,4)} s")
    log.info("================ preprocess + scoring ================")

    return {
        "status": "success",
        "aids": payload.aids,
        "event_types": payload.event_types,
        "recommendation": df_features["candidate_aid"].to_list(),
        "scores": df_features[metrics].to_list(),
    }
