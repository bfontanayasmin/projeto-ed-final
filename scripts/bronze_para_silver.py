"""Camada silver: junta as 18 tabelas normalizadas da bronze em 3 tabelas
limpas e prontas para análise, uma por grão do dado.

Origem:  s3a://bronze/prf/<tabela>/          (cópia fiel do schema prf, em 3FN)
Destino: s3a://silver/prf/acidente/          1 linha por acidente   (particionado por ano)
         s3a://silver/prf/veiculo/           1 linha por veículo    (particionado por ano)
         s3a://silver/prf/envolvido/         1 linha por pessoa     (particionado por ano)

O que a silver faz em relação à bronze:
  * troca os ids das tabelas de domínio pelas descrições (município, UF, causa, ...);
  * volta a ter, por acidente, as contagens de vítimas que o 3FN removeu
    (ilesos, feridos_leves, feridos_graves, mortos) calculadas a partir de estado_fisico;
  * acrescenta colunas de data/hora para análise (ano, mes, dia_semana, hora);
  * padroniza valores "sem informação" (ex.: sexo "Ignorado" -> "Não Informado")
    e cria faixas (faixa_etaria, idade_veiculo).

Execução (de dentro do WSL/Ubuntu, na pasta do projeto):
    docker exec ed-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/spark-apps/bronze_para_silver.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE = "s3a://bronze/prf"
SILVER = "s3a://silver/prf"

DIAS_SEMANA = {1: "domingo", 2: "segunda-feira", 3: "terça-feira", 4: "quarta-feira",
               5: "quinta-feira", 6: "sexta-feira", 7: "sábado"}


def ler(spark, tabela):
    return spark.read.parquet(f"{BRONZE}/{tabela}").drop("_ingerido_em")


def faixa_etaria(col):
    return (F.when(col.isNull(), "Não Informado")
            .when(col < 18, "0-17")
            .when(col < 25, "18-24")
            .when(col < 35, "25-34")
            .when(col < 45, "35-44")
            .when(col < 60, "45-59")
            .otherwise("60+"))


def faixa_idade_veiculo(col):
    return (F.when(col.isNull(), "Não Informado")
            .when(col <= 3, "0-3 anos")
            .when(col <= 10, "4-10 anos")
            .when(col <= 20, "11-20 anos")
            .otherwise("21+ anos"))


def gravar(df, nome):
    destino = f"{SILVER}/{nome}"
    df.write.mode("overwrite").partitionBy("ano").parquet(destino)
    total = df.sparkSession.read.parquet(destino).count()
    print(f"[silver] {nome:<10} -> {destino} ({total} linhas)")


def main():
    spark = SparkSession.builder.appName("bronze_para_silver").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    # --- domínios -----------------------------------------------------------
    municipio = ler(spark, "municipio").select("id_municipio", "nome_municipio", F.col("sigla_uf").alias("uf"))
    uop = (ler(spark, "uop_prf")
           .join(ler(spark, "delegacia_prf"), "id_delegacia")
           .join(ler(spark, "regional_prf"), "id_regional")
           .select("id_uop", F.col("codigo_uop").alias("uop"),
                   F.col("codigo_delegacia").alias("delegacia"),
                   F.col("codigo_regional").alias("regional")))
    causa = ler(spark, "causa_acidente").withColumnRenamed("descricao", "causa_acidente")
    tipo_acidente = ler(spark, "tipo_acidente").withColumnRenamed("descricao", "tipo_acidente")
    condicao = ler(spark, "condicao_meteorologica").withColumnRenamed("descricao", "condicao_meteorologica")
    tipo_veiculo = ler(spark, "tipo_veiculo").withColumnRenamed("descricao", "tipo_veiculo")
    modelo = (ler(spark, "modelo_veiculo")
              .join(ler(spark, "fabricante_veiculo"), "id_fabricante")
              .select("id_modelo_veiculo", F.col("nome_fabricante").alias("fabricante"),
                      F.col("nome_modelo").alias("modelo")))
    tipo_envolvido = ler(spark, "tipo_envolvido").withColumnRenamed("descricao", "tipo_envolvido")
    estado_fisico = ler(spark, "estado_fisico").withColumnRenamed("descricao", "estado_fisico")

    # --- envolvido (1 linha por pessoa) ---------------------------------------
    envolvido = (ler(spark, "envolvido")
                 .join(tipo_envolvido, "id_tipo_envolvido")
                 .join(estado_fisico, "id_estado_fisico")
                 .withColumn("sexo", F.when(F.col("sexo").isin("Masculino", "Feminino"), F.col("sexo"))
                             .otherwise(F.lit("Não Informado")))
                 .withColumn("faixa_etaria", faixa_etaria(F.col("idade")))
                 .drop("id_tipo_envolvido", "id_estado_fisico", "atualizado_em"))

    # Contagem de vítimas por acidente e por veículo (colunas derivadas que o 3FN removeu)
    def contagens(df, chave):
        ef = F.col("estado_fisico")
        return df.groupBy(chave).agg(
            F.count("*").alias("qtd_envolvidos"),
            F.sum((ef == "Ileso").cast("int")).alias("ilesos"),
            F.sum((ef == "Lesões Leves").cast("int")).alias("feridos_leves"),
            F.sum((ef == "Lesões Graves").cast("int")).alias("feridos_graves"),
            F.sum((ef == "Óbito").cast("int")).alias("mortos"),
        )

    vitimas_acidente = contagens(envolvido, "id_acidente")
    vitimas_veiculo = contagens(envolvido.where(F.col("id_veiculo").isNotNull()), "id_veiculo")

    # --- traçado da via (N:N) volta a ser uma lista por acidente ----------------
    tracado = (ler(spark, "acidente_tracado_via")
               .join(ler(spark, "tracado_via"), "id_tracado_via")
               .groupBy("id_acidente")
               .agg(F.array_sort(F.collect_list("descricao")).alias("tracado_via_lista"))
               .withColumn("tracado_via", F.concat_ws(";", "tracado_via_lista")))

    veiculos_por_acidente = ler(spark, "veiculo").groupBy("id_acidente").agg(F.count("*").alias("qtd_veiculos"))

    # --- acidente (1 linha por ocorrência) -------------------------------------
    dia_semana = F.create_map(*[x for k, v in DIAS_SEMANA.items() for x in (F.lit(k), F.lit(v))])
    acidente = (ler(spark, "acidente")
                .join(municipio, "id_municipio")
                .join(uop, "id_uop", "left")
                .join(causa, "id_causa_acidente")
                .join(tipo_acidente, "id_tipo_acidente")
                .join(condicao, "id_condicao_meteorologica")
                .join(tracado, "id_acidente", "left")
                .join(vitimas_acidente, "id_acidente", "left")
                .join(veiculos_por_acidente, "id_acidente", "left")
                .na.fill(0, ["qtd_envolvidos", "ilesos", "feridos_leves", "feridos_graves", "mortos", "qtd_veiculos"])
                .withColumn("ano", F.year("data_acidente"))
                .withColumn("mes", F.month("data_acidente"))
                .withColumn("dia_semana_num", F.dayofweek("data_acidente"))
                .withColumn("dia_semana", dia_semana[F.col("dia_semana_num")])
                # TIME do Postgres chega pelo JDBC como timestamp de 1970-01-01: fica só a hora
                .withColumn("hora", F.hour("horario"))
                .withColumn("horario", F.date_format("horario", "HH:mm:ss"))
                .withColumn("acidente_fatal", F.col("mortos") > 0)
                .withColumn("_processado_em", F.current_timestamp())
                .select("id_acidente", "data_acidente", "horario", "ano", "mes", "dia_semana_num", "dia_semana",
                        "hora", "fase_dia", "uf", "nome_municipio", "br", "km", "latitude", "longitude",
                        "regional", "delegacia", "uop", "causa_acidente", "tipo_acidente",
                        "classificacao_acidente", "condicao_meteorologica", "sentido_via", "tipo_pista",
                        "uso_solo_urbano", "tracado_via", "tracado_via_lista", "qtd_veiculos",
                        "qtd_envolvidos", "ilesos", "feridos_leves", "feridos_graves", "mortos",
                        "acidente_fatal", "_processado_em"))
    acidente = acidente.cache()
    gravar(acidente, "acidente")

    ctx = acidente.select("id_acidente", "ano", "data_acidente", "uf")

    # --- veiculo (1 linha por veículo) -----------------------------------------
    veiculo = (ler(spark, "veiculo")
               .join(tipo_veiculo, "id_tipo_veiculo", "left")
               .join(modelo, "id_modelo_veiculo")
               .join(ctx, "id_acidente")
               .join(vitimas_veiculo, "id_veiculo", "left")
               .na.fill(0, ["qtd_envolvidos", "ilesos", "feridos_leves", "feridos_graves", "mortos"])
               .withColumn("tipo_veiculo", F.coalesce("tipo_veiculo", F.lit("Não Informado")))
               .withColumn("idade_veiculo",
                           F.when(F.col("ano") - F.col("ano_fabricacao") >= 0, F.col("ano") - F.col("ano_fabricacao")))
               .withColumn("faixa_idade_veiculo", faixa_idade_veiculo(F.col("idade_veiculo")))
               .withColumn("_processado_em", F.current_timestamp())
               .select("id_veiculo", "id_acidente", "data_acidente", "ano", "uf", "tipo_veiculo", "fabricante",
                       "modelo", "ano_fabricacao", "idade_veiculo", "faixa_idade_veiculo", "qtd_envolvidos",
                       "ilesos", "feridos_leves", "feridos_graves", "mortos", "_processado_em"))
    gravar(veiculo, "veiculo")

    # --- envolvido com contexto do acidente e do veículo -----------------------
    envolvido_silver = (envolvido
                        .join(ctx, "id_acidente")
                        .join(ler(spark, "veiculo").join(tipo_veiculo, "id_tipo_veiculo", "left")
                              .select("id_veiculo", "tipo_veiculo"), "id_veiculo", "left")
                        .withColumn("tipo_veiculo",
                                    F.when(F.col("id_veiculo").isNull(), F.lit("Sem veículo"))
                                    .otherwise(F.coalesce("tipo_veiculo", F.lit("Não Informado"))))
                        .withColumn("_processado_em", F.current_timestamp())
                        .select("id_pessoa", "id_acidente", "id_veiculo", "data_acidente", "ano", "uf",
                                "tipo_envolvido", "estado_fisico", "idade", "faixa_etaria", "sexo",
                                "tipo_veiculo", "_processado_em"))
    gravar(envolvido_silver, "envolvido")

    spark.stop()


if __name__ == "__main__":
    main()
