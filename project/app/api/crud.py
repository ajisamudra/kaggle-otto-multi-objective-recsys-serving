from typing import List

from tortoise.expressions import Q

from app.data_models.tortoise import (
    Item_Covisit_Buy2Buy_Weight,
    Item_Covisit_Buy_Weight,
    Item_Covisit_Click_Weight,
    Item_Feature,
    Item_Hour_Feature,
    Item_Weekday_Feature,
)


async def retrieve_item_features(candidate_aids: List) -> List:
    results = await Item_Feature.filter(aid__in=candidate_aids).values_list()
    return results


async def retrieve_item_hour_features(candidate_aids: List, hours: List) -> List:
    results = await Item_Hour_Feature.filter(
        Q(aid__in=candidate_aids) & Q(hour__in=hours)
    ).values_list(
        "aid",
        "hour",
        "itemXhour_click_count",
        "itemXhour_click_to_cart_cvr",
        "itemXhour_frac_click_all_click_count",
        "itemXhour_frac_click_all_hour_click_count",
    )
    return results


async def retrieve_item_weekday_features(candidate_aids: List, weekdays: List) -> List:
    results = await Item_Weekday_Feature.filter(
        Q(aid__in=candidate_aids) & Q(weekday__in=weekdays)
    ).values_list(
        "aid",
        "weekday",
        "itemXweekday_all_events_count",
        "itemXweekday_cart_count",
        "itemXweekday_click_to_cart_cvr",
        "itemXweekday_cart_to_order_cvr",
        "itemXweekday_frac_click_all_click_count",
        "itemXweekday_frac_cart_all_cart_count",
        "itemXweekday_frac_cart_all_weekday_cart_count",
    )
    return results


async def retrieve_item_covisit_click_weights(candidate_aids: List) -> List:
    results = await Item_Covisit_Click_Weight.filter(
        aid_x__in=candidate_aids
    ).values_list("aid_x", "aid_y", "wgt")
    return results


async def retrieve_item_covisit_buys_weights(candidate_aids: List) -> List:
    results = await Item_Covisit_Buy_Weight.filter(
        aid_x__in=candidate_aids
    ).values_list("aid_x", "aid_y", "wgt")
    return results


async def retrieve_item_covisit_buy2buy_weights(candidate_aids: List) -> List:
    results = await Item_Covisit_Buy2Buy_Weight.filter(
        aid_x__in=candidate_aids
    ).values_list("aid_x", "aid_y", "wgt")
    return results
