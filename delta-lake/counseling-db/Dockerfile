FROM mongo:latest
ENV MONGO_INITDB_ROOT_USERNAME root
ENV MONGO_INITDB_ROOT_PASSWORD root
ENV MONGO_INITDB_DATABASE counselors
ENV MONGO_INITDB_DATABASE meeting_requests
ENV MONGO_INITDB_DATABASE meeting_reports

COPY init.js /docker-entrypoint-initdb.d/