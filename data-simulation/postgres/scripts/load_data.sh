#!/bin/bash

echo "📥 Checking if mock data exists..."

DATA_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT COUNT(*) FROM students;")

if [ "$DATA_EXISTS" -eq "0" ]; then
    echo "📥 No data found. Loading mock data into PostgreSQL..."
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /home/load_data.sql
    echo "✅ Mock data inserted successfully!"
else
    echo "✅ Mock data already exists. Skipping insertion."
fi