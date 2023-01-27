# project/app/api/ping.py
from fastapi import APIRouter, Depends

from app.models.pydantic import PayloadSchema, ResponseSchema
from app.preprocess.retrieve_and_make_features import retrieve_and_make_features

router = APIRouter()


@router.post("/predict", response_model=ResponseSchema,)
async def predict(payload:PayloadSchema):
    df_features = retrieve_and_make_features(payload=payload)
    # predict: input df_features output dict of recommendation with its score
    return {
        "status": "success",
        "aids": payload.aids,
        "event_types": payload.event_types,
        "recommendation": sorted(payload.aids, reverse=True),
        "scores": sorted(payload.aids, reverse=True),
    }
