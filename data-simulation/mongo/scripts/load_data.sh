#!/bin/bash

# mongoimport --db='counseling' --collection='counselors' --file='/data/counselor_profiles.json' --jsonArray --username='root' --password='root' --authenticationDatabase=admin

# Check if the collection already exists and has data
count=$(mongosh --quiet --username root --password root --authenticationDatabase admin --eval "db.counselors.countDocuments()" counseling)

if [ "$count" -eq 0 ]; then
    echo "📥 Loading mock data into the counselors collection..."
    mongoimport --db='counseling' --collection='counselors' --file='/data/counselor_profiles.json' --jsonArray --username='root' --password='root' --authenticationDatabase=admin
    echo "📥 Loading mock data into the meeting_requests collection..."
    mongoimport --db='counseling' --collection='meeting_requests' --file='/data/past_meeting_requests.json' --jsonArray --username='root' --password='root' --authenticationDatabase=admin
    echo "📥 Loading mock data into the meeting_reports collection..."
    mongoimport --db='counseling' --collection='meeting_reports' --file='/data/past_meeting_reports.json' --jsonArray --username='root' --password='root' --authenticationDatabase=admin
    echo "✅ Mock data inserted successfully!"
else
    echo "✅ Counselors collection already populated. Skipping data import."
fi
