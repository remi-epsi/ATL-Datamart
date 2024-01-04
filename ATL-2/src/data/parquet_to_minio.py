from minio import Minio
from minio.error import S3Error
import pyarrow.parquet as pq
import io
import os

'''

Objectif : Intégrer les fichiers *.parquet dans Minio 


'''

def main():

    #Paramétrer l'accès à Minio
    minio_endpoint = 'localhost:9000'
    minio_access_key = 'minio'
    minio_secret_key = 'minio123'
    minio_bucket_name = 'parquet'

    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False
    )

    if not minio_client.bucket_exists(minio_bucket_name):
        minio_client.make_bucket(minio_bucket_name)

    #Recupérer les différents fichier parquet stockés en locale
    local_parquet_folder = "data\\raw"
    parquet_files = [f for f in os.listdir(local_parquet_folder) if f.endswith('.parquet')]

    #Intégration des fichiers Parquet dans Minio

    for parquet_file_name in parquet_files:
        
        local_parquet_file = os.path.join(local_parquet_folder, parquet_file_name)
        parquet_file = pq.read_table(local_parquet_file)
        buffer = io.BytesIO()
        pq.write_table(parquet_file, buffer)
        buffer.seek(0)
        minio_object_name = f'parquet/{parquet_file_name}'

        try:
            minio_client.put_object(
                minio_bucket_name,
                minio_object_name,
                buffer,
                buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            print(f"Le fichier Parquet '{parquet_file_name}' a été téléversé avec succès dans Minio: {minio_object_name}")
        except S3Error as e:
            print(f"Erreur lors du téléversement dans Minio: {e}")

if __name__ == "__main__":
     main()
