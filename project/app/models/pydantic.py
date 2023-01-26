from pydantic import BaseModel
from typing import List

class PayloadSchema(BaseModel):
    aids: List[int]
    timestamps: List[int]
    event_types: List[int]

class ResponseSchema(BaseModel):
    status: str
    aids: List[int]
    event_types: List[int]
    click_recs: List[int]
    cart_recs: List[int]
    order_recs: List[int]

