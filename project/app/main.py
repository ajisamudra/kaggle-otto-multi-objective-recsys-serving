# project/app/main.py

import logging

from fastapi import FastAPI

from app.api import health, predict
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
# - load 1 model
# - load annoy index for candidate retriever
# - init_db for accessing batch features item, item-day, item-weekday

# TODO:
# create simple working model inference with following requirements
# - infer using 1 model
# - only include past aids as candiates
# - build preprocess from past aids to final features for model
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
    # flight_model_loader = FlightModelLoader(config=SERVICE_CONFIG)
    # flight_ranking = flight_model_loader.init_ranking()
    # flight_propensity = flight_model_loader.init_propensity()
    log.info("successfully instantiate ranker-ml")


@app.on_event("shutdown")
async def shutdown_event():
    log.info("Shutting down...")
