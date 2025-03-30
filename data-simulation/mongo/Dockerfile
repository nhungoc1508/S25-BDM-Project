FROM mongo:latest
ENV MONGO_INITDB_ROOT_USERNAME root
ENV MONGO_INITDB_ROOT_PASSWORD root
ENV MONGO_INITDB_DATABASE counselors

COPY init.js /docker-entrypoint-initdb.d/
COPY scripts/load_data.sh /docker-entrypoint-initdb.d/
COPY data/ /data/

RUN chmod +x /docker-entrypoint-initdb.d/load_data.sh