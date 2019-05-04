from pymongo import MongoClient

client = MongoClient(port=27017)
DB = client.get_database('Airbnb')
collection = DB.get_collection('Airbnb_data')
print("Connected to ", DB)

print(collection.distinct('fields.country_code'))

record_count = 10
query1 = {"fields.country_code":"US","fields.smart_location":"San Francisco, CA","fields.review_scores_cleanliness":10}

print("\n1. List all the Airbnb in San Francisco with cleanliness reviews 10\n")
cursor1 = collection.find(query1, {'_id': 0, 'fields.listing_url': 1, 'fields.review_scores_cleanliness': 1}).limit(record_count)
print("cursor1.count() is ", cursor1.count)
for cur in cursor1:
   print(cur)

print("\n2. Count the host listings in San Francisco")
query2 = {"fields.country_code":"US", "fields.smart_location":"San Francisco, CA"}
print("The count of the host listings in San Francisco is ", collection.find(query2).count())

print("\n3. Find all the hosts in New York city that respond within an hour")
query3 = {"fields.country_code":"US",
        "fields.smart_location":"New York, NY",
	"fields.host_response_time" : "within an hour"}
cursor3 = collection.find(query3, {'_id': 0, 'fields.listing_url': 1, 'fields.smart_location': 1, 'fields.host_response_time':1 }).limit(record_count)
print("cursor1.count() is ", cursor3.count)
for cur in cursor3:
   print(cur)

print("\n4. Find Airbnb with the maximum number of reviews")
cursor4 = collection.aggregate([{'$group':{'_id':'fields.number_of_reviews','maxReviews': { '$max': '$fields.number_of_reviews'}}}])
for document in cursor4:
    print("The Airbnb with the maximum number of reviews is ", document)


print("\n5. Find the minimum deposit")
cursor5 = collection.aggregate([{'$group':{'_id':'fields.security_deposit','minDeposit': { '$min': '$fields.security_deposit' }}}])
for document in cursor5:
    print("The minimum deposit is ", document)
