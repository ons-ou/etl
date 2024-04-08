from google.cloud import bigquery

class BigQueryEPAData:
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "bigquery-public-data"
        self.dataset_id = "epa_historical_air_quality"
        self.dataset_ref = self.project_id + "." + self.dataset_id
        self.dataset = self.client.get_dataset(self.dataset_ref)

    def get_last_modified_time(self):
        return self.dataset.modified

    def get_data_as_dataframe(self, table_id, columns='*'):
        table_ref = self.dataset_ref.table(table_id)
        table = self.client.get_table(table_ref)
        query = f"SELECT {columns} FROM `{self.project_id}.{self.dataset_id}.{table_id}`"
        df = self.client.query(query).to_dataframe()
        return df
