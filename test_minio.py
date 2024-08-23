from minio import Minio
from minio.error import S3Error

def test_minio():
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    bucket_name = "novo-bucket"
    object_name = "test.txt"
    file_content = "Este é um arquivo teste"

    try:
        # Verifica se o bucket existe, caso contrário, cria um novo
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' criado com sucesso!")
        else:
            print(f"Bucket '{bucket_name}' já existe...")

        # Cria um arquivo localmente
        with open(object_name, "w") as file:
            file.write(file_content)

        # Upload do arquivo para o bucket
        client.fput_object(bucket_name, object_name, object_name)
        print(f"Arquivo '{object_name}' enviado com sucesso!")

        # Download do arquivo do bucket
        client.fget_object(bucket_name, object_name, f"downloaded_{object_name}")
        print(f"Arquivo '{object_name}' baixado com sucesso!")

    except S3Error as err:
        print(f"Erro: {err}")

if __name__ == "__main__":
    test_minio()
