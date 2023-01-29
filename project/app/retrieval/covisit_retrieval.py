import logging
from typing import List, Tuple


from starlette.requests import Request

from app.data_models.pydantic import PayloadSchema
from app.embeddings.covisit import suggest_candidates_covisit
from app.api.crud import (
    retrieve_item_covisit_buys_weights,
    retrieve_item_covisit_buy2buy_weights,
)
from utils import constants

log = logging.getLogger("uvicorn")


async def covisit_retrieval(
    payload: PayloadSchema, request: Request
) -> Tuple[List, List]:

    # retrieve covisit_buys
    results = await retrieve_item_covisit_buys_weights(candidate_aids=payload.aids)

    covisit_buys = {}
    # the result will have 3 columns: aid_x, aid_y, wgt
    for aid_x, aid_y, wgt in results:
        val = covisit_buys.get(aid_x, None)
        if val is None:
            covisit_buys[aid_x] = [aid_y]
        else:
            val.append(aid_y)
            covisit_buys[aid_x] = val

    # retrieve covisit buy2buy
    results = await retrieve_item_covisit_buy2buy_weights(candidate_aids=payload.aids)

    covisit_buy2buy = {}
    # the result will have 3 columns: aid_x, aid_y, wgt
    for aid_x, aid_y, wgt in results:
        val = covisit_buy2buy.get(aid_x, None)
        if val is None:
            covisit_buy2buy[aid_x] = [aid_y]
        else:
            val.append(aid_y)
            covisit_buy2buy[aid_x] = val

    candidates, ranks = suggest_candidates_covisit(
        n_past_aids_cand=constants.N_PAST_AID,
        n_covisit_cand=constants.N_COVISIT,
        aids=payload.aids,
        event_types=payload.event_types,
        covisit_buy2buy=covisit_buy2buy,
        covisit_buys=covisit_buys,
    )

    return candidates, ranks
