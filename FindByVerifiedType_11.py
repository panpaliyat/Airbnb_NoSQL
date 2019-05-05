from pymongo import MongoClient

# Connect to MongoDB installed on the locahost
client = MongoClient(port=27017)
DB = client.get_database('Airbnb_2')
collection = DB.get_collection('testCollection')
print("Connected to ",DB)

# 12. Find Host which are verified by Email and Phone both and have identity verified

country_code = input("Enter the country code")
city = input("Enter the City")
#verified_type = input("Verified Type")
record_count = int(input("Number of records to output"))

query = {'country_code' : country_code, 'city' : city, 'host_identity_verified': "True",
         'host_verifications' : {"$all" : ['email','phone']}}

cursor = collection.find(query,{'_id':0,'listing_url':1,'host_verifications':1}).limit(record_count)

print("Found ",cursor.count()," Records")

for record in cursor:
    print(record)