import itertools
from collections import Counter

import numpy as np

############
# Covisit Retrieval
############


def suggest_candidates_covisit(
    n_past_aids_cand: int,
    n_covisit_cand: int,
    aids: list,
    event_types: list,
    covisit_buys: dict,
    covisit_buy2buy: dict,
):
    type_weight_multipliers = {0: 0.5, 1: 9, 2: 0.5}
    unique_buys = []
    unique_aids = list(dict.fromkeys(aids[::-1]))
    types = event_types
    for ix, aid in enumerate(aids):
        curr_type = types[ix]
        if (curr_type == 1) or (curr_type == 2):
            unique_buys.append(aid)

    # reverse the order
    unique_buys = list(dict.fromkeys(unique_buys[::-1]))

    # past aids
    # RERANK CANDIDATES USING WEIGHTS
    if len(unique_aids) > 20:
        weights = np.logspace(0.1, 1, len(aids), base=2, endpoint=True) - 1
        aids_temp = Counter()
        # RERANK BASED ON REPEAT ITEMS AND TYPE OF ITEMS
        for aid, w, t in zip(aids, weights, types):
            aids_temp[aid] += w * type_weight_multipliers[t]
        past_aid_candidate = [k for k, v in aids_temp.most_common(n_past_aids_cand)]

    else:
        past_aid_candidate = list(unique_aids)[:n_past_aids_cand]

    # covisit candidate
    # RERANK CANDIDATES USING WEIGHTS
    if len(unique_aids) >= 20:
        # USE "CART ORDER" CO-VISITATION MATRIX
        aids2 = list(
            itertools.chain(
                *[covisit_buys[aid] for aid in unique_aids if aid in covisit_buys]
            )
        )

        # RERANK CANDIDATES USING "BUY2BUY" CO-VISITATION MATRIX
        aids3 = list(
            itertools.chain(
                *[covisit_buy2buy[aid] for aid in unique_buys if aid in covisit_buy2buy]
            )
        )

        # RERANK CANDIDATES
        top_aids2 = [
            aid2
            for aid2, cnt in Counter(aids2 + aids3).most_common(n_covisit_cand)
            if aid2 not in unique_aids
        ]
        covisit_candidate = top_aids2[:n_covisit_cand]

    else:
        # USE "CART ORDER" CO-VISITATION MATRIX
        aids2 = list(
            itertools.chain(
                *[covisit_buys[aid] for aid in unique_aids if aid in covisit_buys]
            )
        )
        # USE "BUY2BUY" CO-VISITATION MATRIX
        aids3 = list(
            itertools.chain(
                *[covisit_buy2buy[aid] for aid in unique_buys if aid in covisit_buy2buy]
            )
        )
        # RERANK CANDIDATES
        top_aids2 = [
            aid2
            for aid2, cnt in Counter(aids2 + aids3).most_common(n_covisit_cand)
            if aid2 not in unique_aids
        ]
        covisit_candidate = top_aids2[:n_covisit_cand]

    # final candidates
    candidates = past_aid_candidate + covisit_candidate
    ranks = [i for i in range(len(candidates))]

    return candidates, ranks
