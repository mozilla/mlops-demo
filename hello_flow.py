from metaflow import FlowSpec, step
from sklearn import datasets
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score


@pypi('scikit-learn')
class SklearnFlow(FlowSpec):

    @step
    def start(self):
        self.iris = datasets.load_iris()
        self.X = self.iris['data']
        self.y = self.iris['target']
        self.next(self.rf_model)

    @step
    def rf_model(self):
        self.clf = RandomForestClassifier(
            n_estimators=10,
            max_depth=None,
            min_samples_split=2,
            random_state=0
        )
        self.next(self.train)

    @step
    def train(self):
        self.scores = cross_val_score(self.clf, self.X,
                                      self.y, cv=5)
        self.next(self.end)

    @step
    def end(self):
        print("SklearnFlow is all done.")


if __name__ == "__main__":
    SklearnFlow()
