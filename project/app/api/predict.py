# project/app/api/ping.py
import polars as pl
from fastapi import APIRouter, Depends
from starlette.requests import Request

from app.data_models.pydantic import PayloadSchema, ResponseSchema
from app.preprocess.retrieve_and_make_features import retrieve_and_make_features

router = APIRouter()


@router.post(
    "/predict",
    response_model=ResponseSchema,
)
async def predict(request: Request, payload: PayloadSchema):

    # retrieve the ranker_ml object from startup
    ranker_ml = request.app.state.ranker_ml

    # do retrieval candidates & enrich features for these candidates
    df_features = retrieve_and_make_features(request=request, payload=payload)

    # select features
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

    return {
        "status": "success",
        "aids": payload.aids,
        "event_types": payload.event_types,
        "recommendation": df_features["candidate_aid"].to_list(),
        "scores": df_features[metrics].to_list(),
    }
