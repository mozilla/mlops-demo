import os

from metaflow import FlowSpec, Parameter, card, current, step, pypi, environment
from metaflow.cards import Image

class TrainingFlowBQ(FlowSpec):
    """
    A flow that fetches data from BigQuery to be used in other steps.

    Run this flow to validate that BQ jobs are running correctly.

    Want to run this locally? Use
    
    `python training-flow.py --metadata=local --environment=pypi run --offline True`

    """

    offline_wandb = Parameter(
        "offline",
        help="Do not connect to W&B servers when training",
        type=bool,
        default=False
    )

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print(f"Running the training flow.\nWANDB offline mode: {self.offline_wandb}")
        self.next(self.fetch_bq_data)

    @card
    @pypi(packages={
        "db-dtypes": "1.2.0",  # Required for pandas + BQ
        "google-cloud-bigquery": "3.17.2",
        "matplotlib": "3.8.3", # Required for plotting
        "pandas": "2.2.0",  # Required for rows.to_dataframe()
    })
    @step
    def fetch_bq_data(self):
        """
        A step for fetching data from big query.

        When running this step locally, users are required to
        authenticate with BigQuery, otherwise a permission error
        (403) will be reported. See
        https://cloud.google.com/bigquery/docs/authentication/getting-started#python

        When running this remotely, authentication will be taken care
        of automatically.
        """
        from google.cloud import bigquery
        client = bigquery.Client(project="moz-fx-mfouterbounds-nonp-dbc1")

        # Perform a query.
        EVENTS_QUERY = '''
            WITH events AS (
            SELECT
                event_timestamp,
                event,
                event_extra,
            FROM mozdata.glean_dictionary.events_stream AS e
            WHERE
                DATE(submission_timestamp) >= "2024-01-01"
                AND DATE(submission_timestamp) <= "2024-02-28"
                AND event_timestamp IS NOT NULL
            )
            SELECT * FROM events
        '''

        query_job = client.query(EVENTS_QUERY)
        rows = query_job.result()

        # This stores the data for the next step.
        self.event_data = rows.to_dataframe()

        # Truncate the timestamp to hours.abs
        self.event_data["event_timestamp"] = self.event_data["event_timestamp"].dt.floor("h")
        plot = self.event_data\
            .set_index(["event_timestamp", "event"])\
            .groupby(level=0)\
            .size()\
            .sort_index(ascending=True)\
            .plot(title="Number of events over time")

        current.card.append(Image.from_matplotlib(plot.get_figure()))

        self.next(self.forecast)

    @card
    @pypi(packages={
        "scikit-learn": "1.3.1",
        "matplotlib": "3.8.3",
        "numpy": "1.26.0",
        "pandas": "2.2.0",
        "wandb": "0.16.3",
    })
    @environment(vars={
        "WANDB_API_KEY": os.getenv("WANDB_API_KEY"), 
        "WANDB_NAME": "Plot HistGradientBoostingRegressor",
        "WANDB_ENTITY": os.getenv("WANDB_ENTITY"),
        "WANDB_PROJECT": os.getenv("WANDB_PROJECT")
    })
    @step
    def forecast(self):
        """
        In this step, we forecast events by hour. The produced model
        is very likely to be bad, the point of this step IS NOT to
        produce a great model, but to showcase training over data
        fetched via Big Query.

        This is based on
        https://scikit-learn.org/stable/auto_examples/applications/plot_time_series_lagged_features.html
        """
        import numpy as np
        import pandas as pd
        import matplotlib.pyplot as plt
        import wandb
        from sklearn.ensemble import HistGradientBoostingRegressor
        from sklearn.model_selection import TimeSeriesSplit
        from sklearn.metrics import mean_absolute_percentage_error

        # Note: this step is potentially executed from a different
        # machine, so `os.getenv` will report whatever is fed to
        # the machine via the `@environment` annotation at the
        # `@step` level.
        wandb.init(
            entity=os.getenv("WANDB_ENTITY"),
            project=os.getenv("WANDB_PROJECT"),
            mode="offline" if self.offline_wandb else "online",
        )

        df = self.event_data
        counts_series = df\
            .set_index(['event_timestamp', 'event'])\
            .groupby(level=0)\
            .size()\
            .sort_index(ascending=True)

        lagged_df = pd.concat(
            [
                counts_series.rename("count"),
                counts_series.shift(1).rename("lagged_count_1h"),
            ],
            axis="columns",
        )

        lagged_df = lagged_df.dropna()
        X = lagged_df.drop("count", axis="columns")
        y = lagged_df["count"]
        print("X shape: {}\ny shape: {}".format(X.shape, y.shape))

        ts_cv = TimeSeriesSplit(
            n_splits=6,
            gap=24,
            max_train_size=10000,
            test_size=60,
        )
        all_splits = list(ts_cv.split(X, y))

        train_idx, test_idx = all_splits[0]
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        self.model = HistGradientBoostingRegressor().fit(X_train, y_train)
        wandb.sklearn.plot_summary_metrics(
            self.model, X_train, y_train, X_test, y_test
        )
        y_pred = self.model.predict(X_test)
        mean_absolute_percentage_error(y_test, y_pred)

        # Build a fancy plot.
        all_splits = list(ts_cv.split(X, y))
        train_idx, test_idx = all_splits[0]

        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        max_iter = 50
        gbrt_mean_poisson = HistGradientBoostingRegressor(loss="poisson", max_iter=max_iter)
        gbrt_mean_poisson.fit(X_train, y_train)
        mean_predictions = gbrt_mean_poisson.predict(X_test)

        gbrt_median = HistGradientBoostingRegressor(
            loss="quantile", quantile=0.5, max_iter=max_iter
        )
        gbrt_median.fit(X_train, y_train)
        median_predictions = gbrt_median.predict(X_test)

        gbrt_percentile_5 = HistGradientBoostingRegressor(
            loss="quantile", quantile=0.05, max_iter=max_iter
        )
        gbrt_percentile_5.fit(X_train, y_train)
        percentile_5_predictions = gbrt_percentile_5.predict(X_test)

        gbrt_percentile_95 = HistGradientBoostingRegressor(
            loss="quantile", quantile=0.95, max_iter=max_iter
        )
        gbrt_percentile_95.fit(X_train, y_train)
        percentile_95_predictions = gbrt_percentile_95.predict(X_test)

        last_hours = slice(-96, None)
        fig, ax = plt.subplots(figsize=(15, 7))
        plt.title("Predictions by regression models")
        ax.plot(
            y_test.values[last_hours],
            "x-",
            alpha=0.2,
            label="Actual demand",
            color="black",
        )
        ax.plot(
            median_predictions[last_hours],
            "^-",
            label="GBRT median",
        )
        ax.plot(
            mean_predictions[last_hours],
            "x-",
            label="GBRT mean (Poisson)",
        )
        ax.fill_between(
            np.arange(60),
            percentile_5_predictions[last_hours],
            percentile_95_predictions[last_hours],
            alpha=0.3,
            label="GBRT 90% interval",
        )
        _ = ax.legend()
        current.card.append(Image.from_matplotlib(fig))
        wandb.finish()

        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("TrainingFlowBQ is all done.")


if __name__ == "__main__":
    TrainingFlowBQ()
