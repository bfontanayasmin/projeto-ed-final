"""Copia as 18 tabelas normalizadas do schema prf (Postgres de origem) para a
camada bronze do data lake no MinIO, em Parquet.

Destino: s3a://bronze/prf/<tabela>/

Execução (de dentro do WSL/Ubuntu, na pasta do projeto):
    docker exec ed-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/spark-apps/postgres_para_bronze.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABELAS = [
    # domínio e hierarquia
    "uf", "municipio", "regional_prf", "delegacia_prf", "uop_prf",
    "causa_acidente", "tipo_acidente", "condicao_meteorologica", "tracado_via",
    "tipo_veiculo", "fabricante_veiculo", "modelo_veiculo",
    "tipo_envolvido", "estado_fisico",
    # principais e associativa
    "acidente", "acidente_tracado_via", "veiculo", "envolvido",
]

# Tabelas grandes são lidas em paralelo, particionando pela chave primária
PARTICIONAMENTO = {
    "acidente": "id_acidente",
    "acidente_tracado_via": "id_acidente",
    "veiculo": "id_veiculo",
    "envolvido": "id_pessoa",
}


def main():
    spark = SparkSession.builder.appName("postgres_para_bronze").getOrCreate()

    jdbc_url = "jdbc:postgresql://{host}:{port}/{db}".format(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        db=os.environ["POSTGRES_DB"],
    )
    opcoes = {
        "url": jdbc_url,
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }

    for tabela in TABELAS:
        leitor = spark.read.format("jdbc").options(**opcoes).option("dbtable", f"prf.{tabela}")

        coluna = PARTICIONAMENTO.get(tabela)
        if coluna:
            limites = (
                spark.read.format("jdbc").options(**opcoes)
                .option("query", f"SELECT MIN({coluna}) AS lo, MAX({coluna}) AS hi FROM prf.{tabela}")
                .load().first()
            )
            leitor = (leitor.option("partitionColumn", coluna)
                      .option("lowerBound", str(limites["lo"]))
                      .option("upperBound", str(limites["hi"]))
                      .option("numPartitions", "4"))

        df = leitor.load().withColumn("_ingerido_em", F.current_timestamp())
        destino = f"s3a://bronze/prf/{tabela}"
        df.write.mode("overwrite").parquet(destino)

        total = spark.read.parquet(destino).count()
        print(f"[bronze] prf.{tabela:<24} -> {destino} ({total} linhas)")

    spark.stop()


if __name__ == "__main__":
    main()
