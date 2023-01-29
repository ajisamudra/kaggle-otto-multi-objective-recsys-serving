import logging

from annoy import AnnoyIndex
from fastapi import FastAPI
from app.db import init_db

from app.api import health, predict
from app.embeddings.word2vec import load_annoy_idx_word2vec_vect32_wdw3_embedding
from app.ml_model.ranker import RankerML

log = logging.getLogger("uvicorn")


def create_application() -> FastAPI:
    application = FastAPI()
    application.include_router(health.router)
    application.include_router(predict.router)

    return application


app = create_application()


@app.on_event("startup")
async def startup_event():
    log.info("Starting up...")
    log.info("register tortoise")
    init_db(app)
    log.info("successfully register tortoise")
    log.info("instantiate ranker-ml")
    app.state.ranker_ml = RankerML()
    app.state.ranker_ml.load()
    log.info("successfully instantiate ranker-ml")
    log.info("instantiate word2vec embedding")
    app.state.word2vec_idx: AnnoyIndex = load_annoy_idx_word2vec_vect32_wdw3_embedding()
    log.info("successfully instantiate word2vec embedding")


@app.on_event("shutdown")
async def shutdown_event():
    log.info("Shutting down...")
