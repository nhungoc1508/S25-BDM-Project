print("Running init.js");
db = db.getSiblingDB('counseling')
db.createCollection('counselors');