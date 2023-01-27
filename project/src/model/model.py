from abc import ABC, abstractmethod
from typing import Union
import pandas as pd
from catboost import CatBoostRanker, Pool


#### ABSTRACT MODEL
class ClassifierModel(ABC):
    @abstractmethod
    def fit(self, X_train, X_val, y_train, y_val):
        pass

    @abstractmethod
    def predict(self, X_test):
        pass

    @abstractmethod
    def get_params(self):
        pass

class RankingModel(ABC):
    @abstractmethod
    def fit(self, X_train, X_val, y_train, y_val):
        pass

    @abstractmethod
    def predict(self, X_test):
        pass

    @abstractmethod
    def get_params(self):
        pass

#### RANKER MODEL
class CATRanker(RankingModel):
    def __init__(self, **kwargs):
        self._early_stopping_rounds = kwargs.pop("early_stopping_rounds", 200)
        self._verbose = kwargs.pop("verbose", 100)

        kwargs["custom_metric"] = ["MAP:top=20", "NDCG:top=20"]
        # kwargs[
        #     "loss_function"
        # ] = "QueryCrossEntropy"Z  # YetiRank, StochasticFilter, StochasticRank,

        self.feature_importances_ = None
        self.best_score_ = 0
        self.hyperprams = {}

        # self._model = CatBoostRanker(loss_function="YetiRankPairwise", **kwargs)
        # self._model = CatBoostRanker(loss_function="PairLogitPairwise", **kwargs)
        # self._model = CatBoostRanker(loss_function="QueryRMSE", **kwargs)
        self._model = CatBoostRanker(**kwargs)
        # self._model = CatBoostRanker(loss_function="PairLogit", **kwargs)

    def fit(self, X_train, X_val, y_train, y_val, group_train, group_val):

        train_pool = Pool(data=X_train, label=y_train, group_id=group_train)

        val_pool = Pool(data=X_val, label=y_val, group_id=group_val)

        self._model.fit(
            train_pool,
            eval_set=val_pool,
            use_best_model=True,
            early_stopping_rounds=self._early_stopping_rounds,
            verbose=self._verbose,
        )

        feature_importance = self._model.get_feature_importance(data=train_pool)

        # feature importance as DataFrame
        self.feature_importances_ = pd.DataFrame(
            {
                "feature": X_train.columns.to_list(),
                "importance": feature_importance,
            }
        ).sort_values(by="importance", ascending=False, ignore_index=True)

        # best_score as float
        self.best_score_ = self._model.get_best_score()
        self.hyperprams = self._model.get_all_params()

        return self

    def predict(self, X_test):
        return self._model.predict(X_test)

    def get_params(self):
        return self.hyperprams


#### ENSEMBLE MODEL
class EnsembleModels:
    """Wrapper for Ensemble Models
    It has list of trained models with different set of training data

    The final prediction score will be average of scores from the list of trained models
    """

    def __init__(self):
        self.list_models = []

    def append(self, model: Union[ClassifierModel, RankingModel]):
        self.list_models.append(model)
        return self

    def predict(self, X):
        # average of score from list_models
        y_preds = pd.DataFrame()
        for ix, model in enumerate(self.list_models):
            y_pred = model.predict(X)
            # convert np ndarray to pd Series
            y_pred = pd.Series(y_pred, name=f"y_pred_{ix}")
            y_preds = pd.concat([y_preds, y_pred], axis=1)
        y_preds = y_preds.mean(axis=1)
        return y_preds
