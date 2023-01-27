from utils import constants
import joblib

class RankerML(object):
    """Class to load the ranker-ml"""

    def __init__(self):
        self.model = None

    def load(self):
        self.model = joblib.load(constants.RANKER_PATH)


