
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks import sql
import io
import tempfile
import os

import logging, sys
logging.basicConfig(stream=sys.stderr,
                    level=logging.INFO,
                    format='%(asctime)s [%(name)s][%(levelname)s] %(message)s')
logging.getLogger('databricks.sdk').setLevel(logging.DEBUG)

def upload_dataframe_to_volume(df, catalog_name, schema_name, volume_name, file_path, file_format="csv"):
    """
    Upload a pandas DataFrame to a Unity Catalog volume
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame to upload
    catalog_name : str
        Name of the catalog
    schema_name : str
        Name of the schema
    volume_name : str
        Name of the volume
    file_path : str
        Path within the volume where the file should be stored
    file_format : str, optional
        Format to save the DataFrame (csv, parquet, json)
    """
    # Initialize the WorkspaceClient
    w = WorkspaceClient(debug_headers=True, debug_truncate_bytes=500)
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_format}") as temp_file:
        temp_path = temp_file.name
    
        try:
            # Save DataFrame to the temporary file
            if file_format.lower() == "csv":
                df.to_csv(temp_path, index=False)
            elif file_format.lower() == "parquet":
                df.to_parquet(temp_path, index=False)
            elif file_format.lower() == "json":
                df.to_json(temp_path, orient="records", lines=True)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            # Ensure the file path starts correctly
            if file_path.startswith('/'):
                file_path = file_path[1:]
            
            volume_file_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{file_path}"
            
            # Create directory if needed
            dir_path = os.path.dirname(volume_file_path)
            if dir_path != f"/Volumes/{catalog_name}/{schema_name}/{volume_name}":
                try:
                    w.files.create_directory(dir_path)
                except:
                    # Directory might already exist
                    pass
            
            # Upload the file
            with open(temp_path, 'rb') as file:
                file_bytes = file.read()
                binary_data = io.BytesIO(file_bytes)
                w.files.upload(volume_file_path, binary_data, overwrite=True)
            
            print(f"DataFrame successfully uploaded to {volume_file_path}")
            return volume_file_path
        
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_path):
                os.unlink(temp_path)

# Helpers to enable o-auth login
from databricks.sql.experimental.oauth_persistence import OAuthToken, OAuthPersistence
class MyOAuthPersistence(OAuthPersistence):
    def persist(self, hostname: str, token: OAuthToken):
        # Save token to a secure storage, e.g., a file or database
        pass

    def read(self, hostname: str):
        # Retrieve the token from storage
        return None

def merge(temp_path:str, target_table:str, key_column, cols:list):
    with sql.connect(
                server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                auth_type       = "databricks-oauth",
                experimental_oauth_persistence=MyOAuthPersistence()) as connection:
        
        # run merge statement
        with connection.cursor() as cursor:
            cursor.execute(f"""
                -- merge into the table upload_sample, the primary key column is Id. Update if keys match but values have changed.
                MERGE WITH SCHEMA EVOLUTION INTO {target_table} t USING (  
                SELECT * FROM parquet.`{temp_path}`
                ) s
                ON t.{key_column} = s.{key_column}
                WHEN MATCHED AND ({" or ".join([f"s.{i} <> t.{i}" for i in cols])}) THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                WHEN NOT MATCHED BY SOURCE THEN DELETE;
            """)
            results = cursor.fetchall()
            for row in results:
                print(f"merge: {row}")
        
        # query the results
        with connection.cursor() as cursor:
            cursor.execute(
                f"""SELECT * FROM {target_table} ORDER BY 1, 2 LIMIT 10;"""
            )
            results = cursor.fetchall()
            for row in results:
                print(row)

# Example usage
if __name__ == "__main__":
    # Create a sample DataFrame
    data = {'Id': [10, 11, 12], 'Name': ['Alice', 'Bob', 'Daniel'], 'Age': [27, 35, 66]}
    df = pd.DataFrame(data)
    
    # Upload to volume
    upload_dataframe_to_volume(
        df=df,
        catalog_name="douglas_moore",
        schema_name="demo",
        volume_name="temp",
        file_path="data/sample.parquet",
        file_format="parquet"
    )
    

    temp_path = "/Volumes/douglas_moore/demo/temp/data/sample.parquet"
    target_table = "douglas_moore.demo.upload_sample"
    
    key_column = 'Id'
    cols = df.columns.to_list()
    cols.remove(key_column)
    print(cols)
    merge(temp_path, target_table, key_column, cols)
