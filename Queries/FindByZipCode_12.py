from pymongo import MongoClient

# Connect to MongoDB installed on the locahost
client = MongoClient(port=27017)
DB = client.get_database('Airbnb')
collection = DB.get_collection('Airbnb_data')
print("Connected to ",DB)

# 12. Find the room URL within the specific zip code

country_code = input("Enter the country code")
city = input("Enter the City")
zipcode = input("Enter the Zip code")
record_count = int(input("Number of records to output"))

query = {'fields.country_code' : country_code, 'fields.city' : city, 'fields.zipcode':  zipcode}

cursor = collection.find(query,{'_id':0,'fields.listing_url':1,'fields.zipcode':1}).limit(record_count)

print("Found ",cursor.count()," Records")

for record in cursor:
    print(record)
