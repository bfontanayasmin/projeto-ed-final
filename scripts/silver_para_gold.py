"""Camada gold em OBT (One Big Table): junta as 3 tabelas da silver numa única
tabela larga, no menor grão disponível, pronta para dashboards, SQL e Databricks.

Origem:  s3a://silver/prf/{acidente,veiculo,envolvido}/
Destino: s3a://gold/prf/obt_acidentes/        (particionado por ano)

Grão: 1 linha por pessoa envolvida, com os dados do acidente e do veículo
repetidos em cada linha. Para nada se perder, também entram:
  * veículos sem nenhuma pessoa registrada  -> 1 linha, colunas de pessoa nulas
  * acidentes sem veículo e sem pessoa      -> 1 linha, colunas de veículo e pessoa nulas
A coluna tipo_registro diz qual é o caso: 'pessoa', 'veiculo_sem_pessoa'
ou 'acidente_sem_veiculo'.

Como somar sem contar duas vezes
--------------------------------
Os dados do acidente se repetem em todas as linhas dele, e os do veículo em
todas as linhas do veículo. Por isso a OBT tem colunas de contagem 0/1:
  conta_acidente  1 em exatamente uma linha de cada acidente
  conta_veiculo   1 em exatamente uma linha de cada veículo
  conta_pessoa    1 em cada linha que é uma pessoa
  pessoa_ilesa, pessoa_ferido_leve, pessoa_ferido_grave, pessoa_obito
                  1 quando a pessoa da linha está nesse estado (0 nas demais)

Exemplos (Spark SQL / Databricks):
  -- acidentes, mortos e índice de mortalidade por ano e UF
  SELECT ano, uf,
         SUM(conta_acidente)                              AS qtd_acidentes,
         SUM(pessoa_obito)                                AS mortos,
         ROUND(SUM(pessoa_obito) * 100.0 / SUM(conta_acidente), 2) AS indice_mortalidade
  FROM obt_acidentes GROUP BY ano, uf;

  -- veículos por tipo
  SELECT tipo_veiculo, SUM(conta_veiculo) AS qtd_veiculos FROM obt_acidentes GROUP BY tipo_veiculo;

  -- trechos críticos (faixas de 10 km)
  SELECT uf, br, trecho_km_inicio, SUM(conta_acidente) AS acidentes, SUM(pessoa_obito) AS mortos
  FROM obt_acidentes WHERE br IS NOT NULL GROUP BY uf, br, trecho_km_inicio ORDER BY mortos DESC;

Execução (de dentro do WSL/Ubuntu, na pasta do projeto):
    docker exec ed-spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /opt/spark-apps/silver_para_gold.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

SILVER = "s3a://silver/prf"
GOLD = "s3a://gold/prf"
NOME_OBT = "obt_acidentes"

COLUNAS_OBT = [
    # identificação e tipo da linha
    "id_acidente", "id_veiculo", "id_pessoa", "tipo_registro",
    # contadores para somar sem duplicar
    "conta_acidente", "conta_veiculo", "conta_pessoa",
    "pessoa_ilesa", "pessoa_ferido_leve", "pessoa_ferido_grave", "pessoa_obito",
    # quando
    "data_acidente", "horario", "ano", "mes", "dia_semana_num", "dia_semana", "hora", "fase_dia",
    # onde
    "uf", "nome_municipio", "br", "km", "trecho_km_inicio", "latitude", "longitude",
    "regional", "delegacia", "uop",
    # o acidente
    "causa_acidente", "tipo_acidente", "classificacao_acidente", "condicao_meteorologica",
    "sentido_via", "tipo_pista", "uso_solo_urbano", "tracado_via", "acidente_fatal",
    "acidente_qtd_veiculos", "acidente_qtd_envolvidos", "acidente_ilesos", "acidente_feridos_leves",
    "acidente_feridos_graves", "acidente_mortos",
    # o veículo
    "tipo_veiculo", "fabricante", "modelo", "ano_fabricacao", "idade_veiculo", "faixa_idade_veiculo",
    "veiculo_qtd_envolvidos", "veiculo_mortos",
    # a pessoa
    "tipo_envolvido", "estado_fisico", "idade", "faixa_etaria", "sexo",
    "_atualizado_em",
]


def main():
    spark = SparkSession.builder.appName("silver_para_gold").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    # --- silver, com prefixo nas medidas de cada grão ---------------------------
    acidente = (spark.read.parquet(f"{SILVER}/acidente")
                .drop("tracado_via_lista", "_processado_em")
                .withColumnRenamed("qtd_veiculos", "acidente_qtd_veiculos")
                .withColumnRenamed("qtd_envolvidos", "acidente_qtd_envolvidos")
                .withColumnRenamed("ilesos", "acidente_ilesos")
                .withColumnRenamed("feridos_leves", "acidente_feridos_leves")
                .withColumnRenamed("feridos_graves", "acidente_feridos_graves")
                .withColumnRenamed("mortos", "acidente_mortos"))

    veiculo = (spark.read.parquet(f"{SILVER}/veiculo")
               .select("id_veiculo", "id_acidente", "tipo_veiculo", "fabricante", "modelo",
                       "ano_fabricacao", "idade_veiculo", "faixa_idade_veiculo",
                       F.col("qtd_envolvidos").alias("veiculo_qtd_envolvidos"),
                       F.col("mortos").alias("veiculo_mortos")))

    envolvido = (spark.read.parquet(f"{SILVER}/envolvido")
                 .select("id_pessoa", "id_acidente", "id_veiculo", "tipo_envolvido",
                         "estado_fisico", "idade", "faixa_etaria", "sexo"))

    # --- esqueleto da OBT: quais linhas existem ---------------------------------
    nulo = F.lit(None).cast("bigint")
    linhas_pessoa = (envolvido.select("id_acidente", "id_veiculo", "id_pessoa")
                     .withColumn("tipo_registro", F.lit("pessoa")))
    linhas_veiculo = (veiculo.select("id_acidente", "id_veiculo")
                      .join(envolvido.select("id_veiculo").where(F.col("id_veiculo").isNotNull()).distinct(),
                            "id_veiculo", "left_anti")
                      .withColumn("id_pessoa", nulo)
                      .withColumn("tipo_registro", F.lit("veiculo_sem_pessoa")))
    com_linha = linhas_pessoa.select("id_acidente").union(linhas_veiculo.select("id_acidente")).distinct()
    linhas_acidente = (acidente.select("id_acidente")
                       .join(com_linha, "id_acidente", "left_anti")
                       .withColumn("id_veiculo", nulo)
                       .withColumn("id_pessoa", nulo)
                       .withColumn("tipo_registro", F.lit("acidente_sem_veiculo")))
    esqueleto = linhas_pessoa.unionByName(linhas_veiculo).unionByName(linhas_acidente)

    # --- junta os atributos dos três grãos ---------------------------------------
    obt = (esqueleto
           .join(acidente, "id_acidente")
           .join(veiculo.drop("id_acidente"), "id_veiculo", "left")
           .join(envolvido.drop("id_acidente", "id_veiculo"), "id_pessoa", "left"))

    # --- contadores 0/1 ----------------------------------------------------------
    ordem = [F.col("id_veiculo").asc_nulls_last(), F.col("id_pessoa").asc_nulls_last()]
    por_acidente = Window.partitionBy("id_acidente").orderBy(*ordem)
    por_veiculo = Window.partitionBy("id_veiculo").orderBy(F.col("id_pessoa").asc_nulls_last())
    estado = F.col("estado_fisico")

    obt = (obt
           .withColumn("conta_acidente", (F.row_number().over(por_acidente) == 1).cast("int"))
           .withColumn("conta_veiculo",
                       F.when(F.col("id_veiculo").isNull(), 0)
                       .otherwise((F.row_number().over(por_veiculo) == 1).cast("int")))
           .withColumn("conta_pessoa", F.col("id_pessoa").isNotNull().cast("int"))
           .withColumn("pessoa_ilesa", F.coalesce((estado == "Ileso").cast("int"), F.lit(0)))
           .withColumn("pessoa_ferido_leve", F.coalesce((estado == "Lesões Leves").cast("int"), F.lit(0)))
           .withColumn("pessoa_ferido_grave", F.coalesce((estado == "Lesões Graves").cast("int"), F.lit(0)))
           .withColumn("pessoa_obito", F.coalesce((estado == "Óbito").cast("int"), F.lit(0)))
           # pedestre/ciclista sem veículo registrado
           .withColumn("tipo_veiculo",
                       F.when(F.col("id_veiculo").isNull() & F.col("id_pessoa").isNotNull(), F.lit("Sem veículo"))
                       .otherwise(F.col("tipo_veiculo")))
           .withColumn("trecho_km_inicio", (F.floor(F.col("km") / 10) * 10).cast("int"))
           .withColumn("_atualizado_em", F.current_timestamp())
           .select(*COLUNAS_OBT))

    destino = f"{GOLD}/{NOME_OBT}"
    obt.write.mode("overwrite").partitionBy("ano").parquet(destino)

    # --- conferência: a OBT tem de reproduzir os totais da silver ----------------
    gravada = spark.read.parquet(destino)
    totais = gravada.agg(F.count("*").alias("linhas"),
                         F.sum("conta_acidente").alias("acidentes"),
                         F.sum("conta_veiculo").alias("veiculos"),
                         F.sum("conta_pessoa").alias("pessoas"),
                         F.sum("pessoa_obito").alias("mortos")).first()
    esperado = {
        "acidentes": acidente.count(),
        "veiculos": veiculo.count(),
        "pessoas": envolvido.count(),
        "mortos": acidente.agg(F.sum("acidente_mortos")).first()[0],
    }
    print(f"[gold] {NOME_OBT} -> {destino} ({totais['linhas']} linhas)")
    divergencias = []
    for chave, valor in esperado.items():
        ok = totais[chave] == valor
        print(f"[gold]   {chave:<10} OBT={totais[chave]:>9}  silver={valor:>9}  {'ok' if ok else 'DIVERGENTE'}")
        if not ok:
            divergencias.append(chave)

    spark.stop()
    if divergencias:
        raise SystemExit(f"OBT não bate com a silver em: {', '.join(divergencias)}")


if __name__ == "__main__":
    main()
