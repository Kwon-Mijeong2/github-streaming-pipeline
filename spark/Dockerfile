FROM apache/spark:3.5.7

USER root

RUN pip install --no-cache-dir \
    psycopg2-binary

USER spark

WORKDIR /opt/spark/work-dir