from metaflow import FlowSpec, step, pypi

class HelloFlowBQ(FlowSpec):
    """
    A flow that fetches data from BigQuery to be used in other steps.

    Run this flow to validate that BQ jobs are running correctly.

    """

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("HelloFlow is starting.")
        self.next(self.fetch_bq_data)

    @pypi(packages={'google-cloud-bigquery': '3.17.2'})
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
        client = bigquery.Client(project="mozdata")

        # Perform a query.

        EVENTS_QUERY = '''
        -- Auto-generated by the Glean Dictionary.
        -- https://docs.telemetry.mozilla.org/cookbooks/accessing_glean_data.html#event-metrics 

        WITH events AS (
        SELECT
            submission_timestamp,
            client_info.client_id,
            event.timestamp AS event_timestamp,
            event.category AS event_category,
            event.name AS event_name,
            event.extra AS event_extra,
        FROM glean_dictionary.events AS e
        CROSS JOIN UNNEST(e.events) AS event
        WHERE
            -- Pick yesterday's data from stable/historical tables.
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html#table-layout-and-naming
            date(submission_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
            AND sample_id = 1 -- 1pct sample for development
            AND event.category = 'glean'
            AND event.name = 'page_load'
        )
        SELECT * FROM events
        -- IMPORTANT: Remove the limit clause when the query is ready.
        LIMIT 10
        '''

        query_job = client.query(EVENTS_QUERY)  # API request
        rows = query_job.result()  # Waits for query to finish

        for row in rows:
            print(row.name)

        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("HelloFlow is all done.")


if __name__ == "__main__":
    HelloFlowBQ()