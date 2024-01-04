import gc
import os
import sys
from minio import Minio
from minio.error import S3Error
from io import BytesIO

import pandas as pd
from sqlalchemy import create_engine

def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.Dataframe) : The dataframe to dump into the DBMS engine

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success: bool = True
            print("Connection successful! Processing parquet file")
            dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')

    except Exception as e:
        success: bool = False
        print(f"Error connection to the database: {e}")
        return success

    return success


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite it columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def main() -> None:

    # Remplacez ces valeurs par vos informations de connexion Minio
    minio_endpoint = 'localhost:9000'
    minio_access_key = 'minio'
    minio_secret_key = 'minio123'
    minio_bucket_name = 'parquet'
    # Initialisez le client Minio
    minio_client = Minio(minio_endpoint,
                        access_key=minio_access_key,
                        secret_key=minio_secret_key,
                        secure=False)
    
    parquet_files = ['yellow_tripdata_2023-01.parquet', 'yellow_tripdata_2023-02.parquet', 'yellow_tripdata_2023-03.parquet','yellow_tripdata_2023-04.parquet','yellow_tripdata_2023-05.parquet','yellow_tripdata_2023-06.parquet','yellow_tripdata_2023-07.parquet','yellow_tripdata_2023-08.parquet']

    for parquet_file in parquet_files:
        '''
        parquet_df: pd.DataFrame = pd.read_parquet(folder_path + parquet_file, engine='pyarrow')
        clean_column_name(parquet_df)
        '''
        file_data = minio_client.get_object(minio_bucket_name, 'parquet/'+parquet_file)
        content = file_data.read()
        # Charger les données depuis le fichier dans un DataFrame pandas (parquet est pris en charge par pandas)
        parquet_df: pd.DataFrame = pd.read_parquet(BytesIO(content))
        clean_column_name(parquet_df)

        if not write_data_postgres(parquet_df):
            del parquet_df
            gc.collect()
            return

        del parquet_df
        gc.collect()


if __name__ == '__main__':
    sys.exit(main())
