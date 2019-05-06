from pymongo import MongoClient

# Connect to MongoDB installed on the locahost
client = MongoClient(port=27017)
DB = client.get_database('Airbnb')
collection = DB.get_collection('Airbnb_data')
print("Connected to ",DB)

# 12. Find Host which allow n extra people

country_code = input("Enter the country code")
city = input("Enter the City")
extra_price = int(input("Price for extra person"))
record_count = int(input("Number of records to output"))

query = {'fields.country_code' : country_code, 'fields.city' : city, 'fields.extra_people': {"$lt" : extra_price ,"$gt" : 0}}

cursor = collection.find(query,{'_id':0,'fields.listing_url':1,'fields.extra_people':1}).limit(record_count).sort('fields.extra_price')

print("Found ",cursor.count()," Records")

for record in cursor:
    print(record)
