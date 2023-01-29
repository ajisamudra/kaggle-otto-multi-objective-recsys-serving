from typing import List

from tortoise.expressions import Q
from app.data_models.tortoise import (
    Item_Features,
    Item_Hour_Features,
    Item_Weekday_Features,
)


async def retrieve_item_features(candidate_aids: List) -> List:
    results = await Item_Features.filter(aid__in=candidate_aids).values()
    return results


async def retrieve_item_hour_features(candidate_aids: List, hours: List) -> List:
    results = await Item_Hour_Features.filter(
        Q(aid__in=candidate_aids) & Q(hour__in=hours)
    ).values()
    return results


async def retrieve_item_weekday_features(candidate_aids: List, weekdays: List) -> List:
    results = await Item_Weekday_Features.filter(
        Q(aid__in=candidate_aids) & Q(weekday__in=weekdays)
    ).values()
    return results
