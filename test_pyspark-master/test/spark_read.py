import pymongo
from kafka import KafkaConsumer


# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Select the database and collection
db = client["treesize_db"]
col = db["col_INFO"]

# Create a document to insert
mydoc = { "name": "John", "address": "Highway 37" }

# Insert the document into the collection
x = col.insert_one(mydoc)

# Print the ID of the inserted document
print(x.inserted_id)


