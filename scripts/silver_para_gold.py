"""Camada gold: indicadores prontos para consumo (dashboards, SQL, Databricks),
calculados a partir das 3 tabelas da silver.

Origem:  s3a://silver/prf/{acidente,veiculo,envolvido}/
Destino: s3a://gold/prf/<indicador>/

Indicadores:
  indicadores_mensais_uf  acidentes, vítimas e veículos por ano, mês e UF
  acidentes_por_causa     acidentes e gravidade por ano e causa
  acidentes_por_tipo      acidentes e gravidade por ano e tipo de acidente
  acidentes_por_horario   acidentes por dia da semana, hora e fase do dia
  condicoes_via           acidentes por clima, tipo de pista e fase do dia
  trechos_criticos        trechos de 10 km de cada BR/UF, com ranking por mortos
  perfil_vitimas          pessoas por ano, tipo de envolvido, faixa etária, sexo e estado físico
  veiculos_envolvidos     veículos por ano, tipo e idade do veículo

Medidas que se repetem:
  indice_mortalidade      mortos a cada 100 acidentes
  pct_acidentes_fatais    % de acidentes com pelo menos 1 morto

Execução (de dentro do WSL/Ubuntu, na pasta do projeto):
    docker exec ed-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/spark-apps/silver_para_gold.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

SILVER = "s3a://silver/prf"
GOLD = "s3a://gold/prf"


def medidas_acidente():
    """Agregações padrão sobre a silver de acidente."""
    return [
        F.count("*").alias("qtd_acidentes"),
        F.sum(F.col("acidente_fatal").cast("int")).alias("acidentes_fatais"),
        F.sum("qtd_veiculos").alias("qtd_veiculos"),
        F.sum("qtd_envolvidos").alias("qtd_envolvidos"),
        F.sum("ilesos").alias("ilesos"),
        F.sum("feridos_leves").alias("feridos_leves"),
        F.sum("feridos_graves").alias("feridos_graves"),
        F.sum("mortos").alias("mortos"),
    ]


def com_taxas(df):
    return (df
            .withColumn("indice_mortalidade", F.round(F.col("mortos") * 100.0 / F.col("qtd_acidentes"), 2))
            .withColumn("pct_acidentes_fatais",
                        F.round(F.col("acidentes_fatais") * 100.0 / F.col("qtd_acidentes"), 2)))


def gravar(df, nome):
    destino = f"{GOLD}/{nome}"
    df = df.withColumn("_atualizado_em", F.current_timestamp())
    # Tabelas pequenas: 1 arquivo por indicador facilita o envio ao Databricks
    df.coalesce(1).write.mode("overwrite").parquet(destino)
    total = df.sparkSession.read.parquet(destino).count()
    print(f"[gold] {nome:<22} -> {destino} ({total} linhas)")


def main():
    spark = SparkSession.builder.appName("silver_para_gold").getOrCreate()

    acidente = spark.read.parquet(f"{SILVER}/acidente").cache()
    veiculo = spark.read.parquet(f"{SILVER}/veiculo")
    envolvido = spark.read.parquet(f"{SILVER}/envolvido")

    # 1. Série mensal por UF
    gravar(com_taxas(acidente.groupBy("ano", "mes", "uf").agg(*medidas_acidente()))
           .orderBy("ano", "mes", "uf"), "indicadores_mensais_uf")

    # 2. Causas
    gravar(com_taxas(acidente.groupBy("ano", "causa_acidente").agg(*medidas_acidente()))
           .orderBy("ano", F.desc("qtd_acidentes")), "acidentes_por_causa")

    # 3. Tipos de acidente
    gravar(com_taxas(acidente.groupBy("ano", "tipo_acidente").agg(*medidas_acidente()))
           .orderBy("ano", F.desc("qtd_acidentes")), "acidentes_por_tipo")

    # 4. Quando acontecem
    gravar(com_taxas(acidente.groupBy("ano", "dia_semana_num", "dia_semana", "hora", "fase_dia")
                     .agg(*medidas_acidente()))
           .orderBy("ano", "dia_semana_num", "hora"), "acidentes_por_horario")

    # 5. Condições da via e do tempo
    gravar(com_taxas(acidente.groupBy("ano", "condicao_meteorologica", "tipo_pista", "fase_dia")
                     .agg(*medidas_acidente()))
           .orderBy("ano", F.desc("qtd_acidentes")), "condicoes_via")

    # 6. Trechos críticos: BR + UF + faixa de 10 km, no período todo
    ranking = Window.orderBy(F.desc("mortos"), F.desc("qtd_acidentes"))
    trechos = (acidente.where(F.col("br").isNotNull() & F.col("km").isNotNull())
               .withColumn("km_inicio", (F.floor(F.col("km") / 10) * 10).cast("int"))
               .withColumn("km_fim", F.col("km_inicio") + 10)
               .groupBy("uf", "br", "km_inicio", "km_fim")
               .agg(*medidas_acidente(),
                    F.first("nome_municipio", ignorenulls=True).alias("municipio_referencia"),
                    F.round(F.avg("latitude"), 6).alias("latitude_media"),
                    F.round(F.avg("longitude"), 6).alias("longitude_media")))
    trechos = com_taxas(trechos).withColumn("ranking_mortos", F.row_number().over(ranking))
    gravar(trechos.orderBy("ranking_mortos"), "trechos_criticos")

    # 7. Perfil das pessoas envolvidas
    gravar(envolvido.groupBy("ano", "tipo_envolvido", "faixa_etaria", "sexo", "estado_fisico")
           .agg(F.count("*").alias("qtd_pessoas"))
           .orderBy("ano", "tipo_envolvido", "faixa_etaria"), "perfil_vitimas")

    # 8. Veículos
    gravar(veiculo.groupBy("ano", "tipo_veiculo", "faixa_idade_veiculo")
           .agg(F.count("*").alias("qtd_veiculos"),
                F.countDistinct("id_acidente").alias("qtd_acidentes"),
                F.sum("qtd_envolvidos").alias("qtd_ocupantes"),
                F.sum("feridos_graves").alias("feridos_graves"),
                F.sum("mortos").alias("mortos"))
           .withColumn("mortos_por_100_veiculos", F.round(F.col("mortos") * 100.0 / F.col("qtd_veiculos"), 2))
           .orderBy("ano", F.desc("qtd_veiculos")), "veiculos_envolvidos")

    spark.stop()


if __name__ == "__main__":
    main()
