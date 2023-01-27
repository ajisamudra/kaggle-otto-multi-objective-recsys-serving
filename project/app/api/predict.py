# project/app/api/ping.py
import polars as pl
from fastapi import APIRouter, Depends

from app.data_models.pydantic import PayloadSchema, ResponseSchema
from app.preprocess.retrieve_and_make_features import retrieve_and_make_features

router = APIRouter()


@router.post(
    "/predict",
    response_model=ResponseSchema,
)
async def predict(payload: PayloadSchema):
    df_features = retrieve_and_make_features(payload=payload)
    # predict: input df_features output dict of recommendation with its score
    # sort candidates aid based on some metrics
    metrics = "sesXaid_type_weighted_log_recency_score"
    df_features = df_features.sort([metrics], reverse=True).head(20)

    return {
        "status": "success",
        "aids": payload.aids,
        "event_types": payload.event_types,
        "recommendation": df_features["candidate_aid"].to_list(),
        "scores": df_features[metrics].to_list(),
    }
