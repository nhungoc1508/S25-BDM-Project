print("Running init.js");
db = db.getSiblingDB('counseling')
db.createCollection('counselors');
db.createCollection('meeting-requests');
db.createCollection('meeting-reports');