# project/app/main.py

import logging

from annoy import AnnoyIndex
from fastapi import FastAPI

from app.api import health, predict
from app.embeddings.covisit import (
    load_top15_covisitation_buy2buy,
    load_top15_covisitation_buys,
)
from app.embeddings.word2vec import load_annoy_idx_word2vec_vect32_wdw3_embedding
from app.ml_model.ranker import RankerML

# from app.db import init_db

log = logging.getLogger("uvicorn")


def create_application() -> FastAPI:
    application = FastAPI()
    application.include_router(health.router)
    application.include_router(predict.router)

    return application


app = create_application()

# on startup
# - load 1 model Done
# - [TODO] load annoy index for candidate retriever & weights/distances features
# - init_db for accessing batch features item, item-day, item-weekday

# TODO:
# - for preprocess will need
#   - covisit weight
#   - word2vec annoy index


@app.on_event("startup")
async def startup_event():
    log.info("Starting up...")

    # init_db(app)
    log.info("instantiate ranker-ml")
    app.state.ranker_ml = RankerML()
    app.state.ranker_ml.load()
    log.info("successfully instantiate ranker-ml")
    # log.info("instantiate word2vec embedding")
    # app.state.word2vec_idx: AnnoyIndex = load_annoy_idx_word2vec_vect32_wdw3_embedding()
    # log.info("successfully instantiate word2vec embedding")
    # log.info("instantiate covisit buys")
    # app.state.buys_dict: dict = load_top15_covisitation_buys()
    # log.info("successfully instantiate covisit buys")
    # log.info("instantiate covisit buy2buy")
    # app.state.buy2buy_dict: dict = load_top15_covisitation_buy2buy()
    # log.info("successfully instantiate covisit buy2buy")


@app.on_event("shutdown")
async def shutdown_event():
    log.info("Shutting down...")
