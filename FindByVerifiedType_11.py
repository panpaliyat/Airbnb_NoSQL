from pymongo import MongoClient

# Connect to MongoDB installed on the locahost
client = MongoClient(port=27017)
DB = client.get_database('Airbnb')
collection = DB.get_collection('Airbnb_data')
print("Connected to ",DB)

# 12. Find Host which are verified by Email and Phone both and have identity verified

country_code = input("Enter the country code")
city = input("Enter the City")
verified_type = input("Verified Type")
record_count = int(input("Number of records to output"))

query = {'fields.country_code' : country_code, 'fields.city' : city, 'fields.host_identity_verified': "True",
         'fields.host_verifications' : verified_type}

cursor = collection.find(query,{'_id':0,'fields.listing_url':1,'fields.host_identity_verified':1}).limit(record_count)

print("Found ",cursor.count()," Records")

for record in cursor:
    print(record)