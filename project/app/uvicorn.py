import uvicorn

from utils import constants

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=constants.APP_HOST,
        port=constants.APP_PORT,
        reload=constants.APP_RELOAD,
    )
