# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio
from minio.error import S3Error
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import os
import urllib.error
import requests
import pyarrow.parquet as pq
import io


def download_parquet():
    # folder_path: str = r'..\..\data\raw'
    # Construct the relative path to the folder
    base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"
    print("TEST DOWNLOAD")
    month: str = pendulum.now().subtract(months=3).format('YYYY-MM')
    nom_fichier = f"yellow_tripdata_{month}.parquet"

    if os.path.exists(f"data\\raw\\{nom_fichier}"):
        print("Le fichier existe déjà")        
    else:       
        url = base_url + nom_fichier
        response = requests.get(url)
        if response.status_code == 200:
            #Télécharger les fichiers sur le PC local
            with open("data\\raw\\"+nom_fichier, "wb") as f:
                f.write(response.content)
            print("Téléchargement du fichier terminé avec succès.")
        else:
            print("Erreur lors du téléchargement. Statut de la réponse :", response.status_code)


# Python Function
def upload_file():
    ###############################################
    # Upload generated file to Minio

    minio_client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    minio_bucket_name: str = 'parquet'

    month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    print(minio_client.list_buckets())

    if not minio_client.bucket_exists(minio_bucket_name):
        minio_client.make_bucket(minio_bucket_name)

    #Recupérer les différents fichier parquet stockés en locale
    local_parquet_file = "data\\raw\\yellow_tripdata_{month}.parquet"

    #Intégration des fichiers Parquet dans Minio     
    parquet_file = pq.read_table(local_parquet_file)
    buffer = io.BytesIO()
    pq.write_table(parquet_file, buffer)
    buffer.seek(0)
    minio_object_name = f'parquet/yellow_tripdata_{month}.parquet'

    try:
        minio_client.put_object(
            minio_bucket_name,
            minio_object_name,
            buffer,
            buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        print(f"Le fichier Parquet 'yellow_tripdata_{month}.parquet' a été téléversé avec succès dans Minio: {minio_object_name}")
    except S3Error as e:
        print(f"Erreur lors du téléversement dans Minio: {e}")

    # On supprime le fichié récement téléchargés, pour éviter la redondance. On suppose qu'en arrivant ici, l'ajout est
    # bien réalisé
    os.remove(os.path.join("./", "yellow_tripdata_" + month + ".parquet"))

###############################################
with DAG(dag_id='Grab_NYC_Data_to_Minio',
         start_date=pendulum.today('UTC').add(days=1),
         schedule=None,
         catchup=False,
         tags=['minio/read/write'],
         ) as dag:
    
    ###############################################
    # Create a task to call your processing function
    t1 = PythonOperator(
        task_id='telechargement_parquet',
        python_callable=download_parquet,
        dag=dag
    )
    t2 = PythonOperator(
        task_id='upload_file_task',
        python_callable=upload_file,
        dag=dag
    )
###############################################  

###############################################
# first upload the file, then read the other file.
t1 >> t2
###############################################

