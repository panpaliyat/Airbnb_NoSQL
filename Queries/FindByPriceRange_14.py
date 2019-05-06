from pymongo import MongoClient

# Connect to MongoDB installed on the locahost
client = MongoClient(port=27017)
DB = client.get_database('Airbnb')
collection = DB.get_collection('Airbnb_data')
print("Connected to ",DB)

# 15. Find the rooms within a specified price range
query = {'fields.country_code' : country_code, 'fields.city' : city, 'fields.price' : {'$gt' : low , '$lt' : high}}
cursor = collection.find(query,{'_id':0,'fields.listing_url':1,'fields.price':1}).limit(record_count).sort('fields.price')

for record in cursor:
    print(record)
