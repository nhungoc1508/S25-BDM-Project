# FROM jupyter/pyspark-notebook:spark-3.4.1
FROM apache/airflow:latest

USER root
# COPY notebooks/example.ipynb .
# RUN apt-get update && apt-get install -y openjdk-8-jdk
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean

ARG NB_USER=jovyan
ARG NB_UID=1000
ARG NB_GID=100

# ENV USER ${NB_USER}
# ENV HOME /home/${NB_USER}
# RUN groupadd -f ${USER} && \
#     chown -R ${USER}:${USER} ${HOME}

# USER ${NB_USER}

# RUN export PACKAGES="io.delta:delta-core_2.12:0.7.0"
# RUN export PYSPARK_SUBMIT_ARGS="--packages ${PACKAGES} pyspark-shell"

# COPY dags/ /usr/local/airflow/dags/
# COPY scripts/ /usr/local/airflow/scripts/

USER airflow
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# RUN /opt/bitnami/spark/venv/bin/pip install --upgrade pip
# COPY requirements.txt .
# RUN /opt/bitnami/spark/venv/bin/pip install -r requirements.txt
# ENV PYSPARK_PYTHON=/opt/bitnami/spark/venv/bin/python

# FROM bitnami/spark:latest
# USER root
# RUN apt-get update && \
#     apt-get install -y openjdk-17-jre-headless && \
#     apt-get clean

# RUN pip install "apache-airflow"

# RUN /opt/bitnami/spark/venv/bin/pip install --upgrade pip
# COPY requirements.txt .
# RUN /opt/bitnami/spark/venv/bin/pip install -r requirements.txt
# ENV PYSPARK_PYTHON=/opt/bitnami/spark/venv/bin/python