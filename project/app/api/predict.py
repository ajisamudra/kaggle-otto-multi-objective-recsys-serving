# project/app/api/ping.py
from fastapi import APIRouter, Depends

from app.models.pydantic import PayloadSchema, ResponseSchema

router = APIRouter()


@router.post("/predict", response_model=ResponseSchema,)
async def predict(payload:PayloadSchema):
    return {
        "status": "success",
        "aids": payload.aids,
        "event_types": payload.event_types,
        "click_recs": sorted(payload.aids, reverse=True),
        "cart_recs": sorted(payload.aids, reverse=True),
        "order_recs": sorted(payload.aids, reverse=False),
    }
