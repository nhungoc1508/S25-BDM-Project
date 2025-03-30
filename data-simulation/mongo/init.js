// // Switch to the appropriate database
// db = db.getSiblingDB('counseling');

// // Drop the existing collection (optional: avoids duplicates on restart)
// db.counselors.drop();

// // Load JSON file from the container's init directory
// const filePath = "/docker-entrypoint-initdb.d/counselor_profiles.json";
// const jsonFile = cat(filePath);
// const jsonData = JSON.parse(jsonFile);

// // Insert the data into the "counselors" collection
// db.counselors.insertMany(jsonData);

// // Print confirmation
// print("✅ Successfully loaded counselor profiles!");

print("Running init.js");
db = db.getSiblingDB('counseling')
db.createCollection('counselors');