import os
import yaml

def yaml_to_dict(yaml_file_path: str):
    config_folder_name = "configs"
    environment = os.getenv("ENVIRONMENT", "dev")
    config_folder = os.path.join(os.getcwd(), config_folder_name, environment)
    filepath = f"{config_folder}/{yaml_file_path}"

    dict_data = {}
    with open(filepath, "r") as f:
        dict_data = yaml.safe_load(f)
        f.close()

    return dict_data

# --------------
# Config
# --------------

SERVICE_CONFIG = yaml_to_dict("serving_config.yaml")

# ---------------
# Retrieval Config
# ---------------

RETRIEVAL_CFG = SERVICE_CONFIG["retrieval"]
N_PAST_AID = RETRIEVAL_CFG.get("n_past_aid_candidates", 10)
N_COVISIT = RETRIEVAL_CFG.get("n_covisit_candidates", 10)
N_WORD2VEC = RETRIEVAL_CFG.get("n_word2vec_candidates", 10)

# ---------------
# API Port
# ---------------
APP_CONFIG = SERVICE_CONFIG["application"]
APP_NAME = APP_CONFIG.get("name", "kaggle-otto-serving")
APP_VERSION = APP_CONFIG.get("version", "0.1.0")
APP_API_PREFIX = APP_CONFIG.get("api_prefix", "/api")
APP_HOST = APP_CONFIG.get("host", "127.0.0.1")
APP_PORT = APP_CONFIG.get("port", 8000)
APP_RELOAD = APP_CONFIG.get("reload", True)

# ---------------
# Pickles Info
# ---------------
RANKER_PATH = os.path.join(os.getcwd(), "pickles", "ranker", "model.pkl")
WORD2VEC_PATH = os.path.join(os.getcwd(), "pickles", "word2vec", "word2vec_local_clicks_skipgram_vec32_wdw3.kv")
COVISIT_PATH = os.path.join(os.getcwd(), "pickles", "co-visitation")

# ---------------
# DB Info
# ---------------
# DATABASE_URL=postgres://{user}:{password}@{hostname}:{port}/{database-name}
DATABASE_URL = os.getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/web_dev")
