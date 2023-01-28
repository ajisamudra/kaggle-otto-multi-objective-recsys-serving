from typing import List, Tuple

from starlette.requests import Request

from app.data_models.pydantic import PayloadSchema
from app.embeddings.covisit import suggest_candidates_covisit


def covisit_retrieval(payload: PayloadSchema, request: Request) -> Tuple[List, List]:
    # candidates, ranks = suggest_candidates_covisit(
    #     n_past_aids_cand=10,  n_covisit_cand=10, aids=payload.aids, event_types=payload.event_types ,covisit_buy2buy=request.app.state.buy2buy_dict, covisit_buys=request.app.state.buys_dict
    # )
    candidates = [170046, 429240, 170046, 588903] + [429240, 597886, 1180285]
    ranks = [i for i in range(len(candidates))]
    return candidates, ranks
