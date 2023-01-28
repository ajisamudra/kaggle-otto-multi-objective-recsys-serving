import logging
import os

from fastapi import FastAPI
from tortoise import Tortoise, run_async
from tortoise.contrib.fastapi import register_tortoise
from utils import constants

log = logging.getLogger("uvicorn")

TORTOISE_ORM = {
    "connections": {"default": constants.DATABASE_URL},
    "apps": {
        "models": {
            "models": ["app.data_models.tortoise", "aerich.models"],
            "default_connection": "default",
        },
    },
}


def init_db(app: FastAPI) -> None:
    register_tortoise(
        app,
        db_url=constants.DATABASE_URL,
        modules={"models": ["app.data_models.tortoise"]},
        generate_schemas=False,
        add_exception_handlers=True,
    )


# new
async def generate_schema() -> None:
    log.info("Initializing Tortoise...")

    await Tortoise.init(
        db_url=constants.DATABASE_URL,
        modules={"models": ["data_models.tortoise"]},
    )
    log.info("Generating database schema via Tortoise...")
    await Tortoise.generate_schemas()
    await Tortoise.close_connections()


# new
if __name__ == "__main__":
    print(constants.DATABASE_URL)
    run_async(generate_schema())
