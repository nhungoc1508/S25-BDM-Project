FROM postgres:latest

# Copy initialization SQL
COPY init.sql /docker-entrypoint-initdb.d/

# Copy the mock data loader script and SQL data
COPY scripts/load_data.sh /docker-entrypoint-initdb.d/
COPY load_data.sql /docker-entrypoint-initdb.d/
COPY data/ /data/

# Ensure the script has execute permissions
RUN chmod +x /docker-entrypoint-initdb.d/load_data.sh