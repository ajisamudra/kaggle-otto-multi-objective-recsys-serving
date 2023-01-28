from typing import List

from pydantic import BaseModel


class PayloadSchema(BaseModel):
    aids: List[int]
    timestamps: List[int]
    event_types: List[int]


class ResponseSchema(BaseModel):
    status: str
    recommendation: List[int]
    scores: List[float]
    aids: List[int]
    event_types: List[int]


class FeaturesSchema(BaseModel):
    candidate_aid: int
    retrieval_covisit: int
    retrieval_word2vec: int
    rank_covisit: int
    rank_word2vec: int
    retrieval_combined: int
    rank_combined: int
    sess_all_events_count: int
    sess_cart_count: int
    sess_clicked_aid_dcount: int
    sess_carted_aid_dcount: int
    sess_ordered_aid_dcount: int
    sess_duration_mins: float
    sess_avg_cart_dur_sec: float
    sess_last_type_in_session: int
    sess_carted_to_ordered_aid_cvr: int
    sess_abs_diff_avg_hour_cart: int
    sess_abs_diff_avg_weekday_cart: int
    sess_binned_aid_dcount: int
    log_rank_covisit_score: float
    frac_log_rank_covisit_score_to_all: float
    click_weight_with_last_event_in_session_aid: float
    click_weight_with_max_recency_event_in_session_aid: float
    click_weight_with_max_weighted_recency_event_in_session_aid: float
    click_weight_with_max_duration_event_in_session_aid: float
    buys_weight_with_last_event_in_session_aid: float
    buys_weight_with_max_recency_event_in_session_aid: float
    buys_weight_with_max_weighted_recency_event_in_session_aid: float
    buys_weight_with_max_duration_event_in_session_aid: float
    buy2buy_weight_with_last_event_in_session_aid: float
    buy2buy_weight_with_max_recency_event_in_session_aid: float
    buy2buy_weight_with_max_weighted_recency_event_in_session_aid: float
    buy2buy_weight_with_max_duration_event_in_session_aid: float
    click_weight_mean: float
    click_weight_max: float
    buys_weight_mean: float
    buys_weight_max: float
    buys_weight_min: float
    buy2buy_weight_mean: float
    buy2buy_weight_max: float
    buy2buy_weight_diff_max_min: float
    buys_weight_diff_max_min: float
    click_weight_diff_max_min: float
    word2vec_skipgram_last_event_cosine_distance: float
    word2vec_skipgram_last_event_euclidean_distance: float
    sesXaid_click_count: int
    sesXaid_cart_count: int
    sesXaid_avg_click_dur_sec: float
    sesXaid_avg_cart_dur_sec: float
    sesXaid_sum_click_dur_sec: int
    sesXaid_sum_dur_sec: int
    sesXaid_type_dcount: int
    sesXaid_log_recency_score: float
    sesXaid_type_weighted_log_recency_score: float
    sesXaid_action_num_reverse_chrono: int
    sesXaid_last_type_in_session: int
    sesXaid_mins_from_last_event: float
    sesXaid_mins_from_last_event_log1p: float
    sesXaid_frac_click_all_click_count: float
    sesXaid_frac_cart_all_cart_count: float
    sesXaid_frac_order_all_order_count: float
    sesXaid_frac_click_all_events_count: float
    sesXaid_frac_cart_all_events_count: float
    sesXaid_frac_order_all_events_count: float
    sesXaid_frac_dur_click_all_dur_sec: float
    sesXaid_frac_dur_cart_all_dur_sec: float
    sesXaid_frac_log_recency_to_all: float
    sesXaid_frac_type_weighted_log_recency_to_all: float
    sesXaid_frac_mins_from_last_event_to_sess_duration: float
    diff_w_mean_click_weight_with_last_event_in_session_aid: float
    relative_diff_w_mean_click_weight_with_last_event_in_session_aid: float
    diff_w_mean_buys_weight_with_last_event_in_session_aid: float
    relative_diff_w_mean_buys_weight_with_last_event_in_session_aid: float
    diff_w_mean_buy2buy_weight_with_last_event_in_session_aid: float
    relative_diff_w_mean_buy2buy_weight_with_last_event_in_session_aid: float
    diff_w_mean_word2vec_skipgram_last_event_cosine_distance: float
    relative_diff_w_mean_word2vec_skipgram_last_event_cosine_distance: float
    diff_w_mean_word2vec_skipgram_last_event_euclidean_distance: float
    relative_diff_w_mean_word2vec_skipgram_last_event_euclidean_distance: float
    item_all_events_count: int
    item_click_count: int
    item_cart_count: int
    item_order_count: int
    item_avg_hour_click: float
    item_avg_hour_cart: float
    item_avg_hour_order: float
    item_avg_weekday_click: float
    item_avg_weekday_order: float
    sessXitem_abs_diff_avg_hour_order: int
    sessXitem_abs_diff_avg_weekday_order: int
    itemXhour_click_count: int
    itemXhour_click_to_cart_cvr: float
    itemXhour_frac_click_all_click_count: float
    itemXhour_frac_click_all_hour_click_count: float
    itemXweekday_all_events_count: int
    itemXweekday_cart_count: int
    itemXweekday_click_to_cart_cvr: float
    itemXweekday_cart_to_order_cvr: float
    itemXweekday_frac_click_all_click_count: float
    itemXweekday_frac_cart_all_cart_count: float
    itemXweekday_frac_cart_all_weekday_cart_count: float


# item features shape (556203, 10)
# item hour features shape (1506694, 6)
# item weekday features shape (1119686, 9)
