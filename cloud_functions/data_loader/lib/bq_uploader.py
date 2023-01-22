from google.cloud import bigquery
import pandas as pd
from lib import Response


class BqUploader:
    def __init__(self,project:str):
        self.project = project
        self.client = bigquery.Client(project=project)

    def upload_dataframe(self, dataset:str,table:str, data:list):
        try:
            df = pd.DataFrame(data)

            dataset_ref = self.client.dataset(dataset)
            table_ref = dataset_ref.table(table)

            result = self.client.load_table_from_dataframe(df, table_ref).result()
        except Exception as e:
            return Response(False,f"Error uploading the dataframe: {e}")
        return Response(True,f"dataframe uploaded: {result.state}")
        