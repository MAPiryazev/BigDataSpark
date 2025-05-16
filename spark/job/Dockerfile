FROM bitnami/spark:3.5
USER root

RUN apt-get update && apt-get install -y curl tree

# Скачиваем JDBC драйверы
RUN curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
 && curl -L -o /opt/bitnami/spark/jars/clickhouse-jdbc-0.8.3-all.jar \
      https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.8.3/clickhouse-jdbc-0.8.3-all.jar

WORKDIR /app

RUN echo "📂 Перед COPY:" && pwd && ls -la

# Создаем структуру директорий
RUN mkdir -p /app/ETL/spark_jobs

# Копируем файл в нужную директорию
COPY spark/job_scripts/test_connection.py /app/ETL/spark_jobs/

RUN echo "📂 После COPY:" && tree /app

ENTRYPOINT ["spark-submit", "--master", "spark://spark-master:7077", "--deploy-mode", "client", "--jars", "/opt/bitnami/spark/jars/clickhouse-jdbc-0.8.3-all.jar,/opt/bitnami/spark/jars/postgresql-42.7.3.jar", "/app/ETL/spark_jobs/test_connection.py"]
