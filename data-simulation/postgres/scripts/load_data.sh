# echo "📥 Checking if mock data exists..."

# DATA_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT COUNT(*) FROM students;")

# if [ "$DATA_EXISTS" -eq "0" ]; then
#     echo "📥 No data found. Loading mock data into PostgreSQL..."
#     psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /home/load_data.sql
#     echo "✅ Mock data inserted successfully!"
# else
#     echo "✅ Mock data already exists. Skipping insertion."
# fi

#!/bin/bash

echo "📥 Checking if table exists..."

# Check if the 'students' table exists
TABLE_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
"SELECT EXISTS (
    SELECT FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name = 'students'
);")

# If the table exists, check if it has any rows
if [ "$TABLE_EXISTS" = "t" ]; then
    DATA_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT COUNT(*) FROM students;")
    if [ "$DATA_EXISTS" -eq "0" ]; then
        echo "📥 Table exists but has no data. Loading mock data into PostgreSQL..."
        psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /home/load_data.sql
        echo "✅ Mock data inserted successfully!"
    else
        echo "✅ Mock data already exists. Skipping insertion."
    fi
else
    echo "📥 Table 'students' does not exist. Loading schema and mock data into PostgreSQL..."
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /home/load_data.sql
    echo "✅ Schema and mock data inserted successfully!"
fi