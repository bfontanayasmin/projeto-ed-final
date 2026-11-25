# Imagem do Spark do projeto: apache/spark:3.5.6 com os conectores que faltam
# na imagem oficial para ler o Postgres (JDBC) e gravar no MinIO (s3a://).
FROM apache/spark:3.5.6

# O Spark 3.5.6 vem com Hadoop 3.3.4: o hadoop-aws precisa ser da mesma versão,
# e o aws-java-sdk-bundle é o que o hadoop-aws 3.3.4 declara como dependência.
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/

# Endpoint e modo de acesso do MinIO (credenciais vêm das variáveis AWS_* do compose).
# A pasta conf é criada antes: se o COPY criar, ela herda o --chmod=644 e fica sem permissão de entrada.
USER root
RUN mkdir -p /opt/spark/conf && chmod 755 /opt/spark/conf
COPY --chmod=644 conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
USER spark
