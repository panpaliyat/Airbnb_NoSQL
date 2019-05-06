
"""
@author: aparnakale
"""

# =============================================================================
#  load libraries
# =============================================================================

from datetime import datetime
from pymongo import MongoClient
from pprint import pprint

class Airbnb:
    def __init__(self):
        country_code,record_count = self.common_input()
        collection = self.make_connection()
        self.country_code = country_code
        self.record_count = record_count
        self.collection = collection
       
    def common_input(self):
        country_code = input("Enter the country code: ")
        record_count = int(input("Number of records to output: "))
        return country_code,record_count

    def take_city(self):
        self.city = input("Enter the city (e.g. San Francisco or Austin): ")
        
    def make_connection(self):
        print('=============================================================================')
        print('Making MongoDB Connection....')
        client = MongoClient('mongodb://172.31.20.226:27019')
        DB = client.get_database('Airbnb')
        collection = DB.get_collection('airbnb_data')
        print("Connected to ",DB)
        return collection

    def print_records(self, cursor):
        print("Found ",cursor.count()," Records")
        for record in cursor:
            print(record)

    # 1. List all the Airbnb in San Francisco with cleanliness reviews 10
    def query1(self):    
        print("Listing all Airbnbs with cleanliness reviews 10..... ")
        self.take_city()
        query = {'country_code' : self.country_code, 'city' : self.city, 
                 'review_scores_cleanliness':10 }
        cursor = self.collection.find(query,{'_id':0,'listing_url':1,'review_scores_cleanliness':1}).limit(self.record_count)
        self.print_records(cursor)
        execStats = self.collection.find(query,{'_id':0,'listing_url':1,'review_scores_cleanliness':1}).explain()['executionStats']
        pprint(execStats)
    
    # 2. Count the host listings in San Francisco
    def query2(self):    
        print("Total number of host listing in the given city are ..... ")
        self.take_city()
        query = {'country_code' : self.country_code, 'city' : self.city}
        cursor = self.collection.find(query).distinct('listing_url')
        print("Records found:",len(cursor))
        

    # 3. Find all the hosts in New York city that respond within an hour
    def query3(self):    
        print("Listing all Airbnbs where hosts respond within an hour ..... ")
        self.take_city()
        query = {'country_code' : self.country_code, 'city' : self.city, 'host_response_time':'within an hour' }
        cursor = self.collection.find(query,{'_id':0,'listing_url':1,'host_name':1}).limit(self.record_count)
        self.print_records(cursor)
        execStats = self.collection.find(query,{'_id':0,'listing_url':1,'host_name':1}).explain()['executionStats']
        pprint(execStats)


    # 4. Find what is the maximum in number of reviews
    def query4(self):    
        print("Find what is the maximum in number of reviews ..... ")
        self.take_city()
        cursor = self.collection.aggregate([{ '$match': {"country_code": self.country_code, 'city' : self.city}},{'$group':{'_id':'number_of_reviews','maxReviews': { '$max': '$number_of_reviews'}}}])
        for document in cursor:
            print("The Airbnb with the maximum number of reviews is ", document)

    # 5. Find the minimum deposit 
    def query5(self):    
        print("Find the minimum deposit  ..... ")
        self.take_city()
        cursor = self.collection.aggregate([{ '$match': {"country_code": self.country_code, 'city' : self.city}},{'$group':{'_id':'security_deposit','minDeposit': { '$min': '$security_deposit' }}}])
        for document in cursor:
            print("The Airbnb with the minimum deposit is ", document)


    # 6. Get all the Airbnbs in San Francisco with cleaning fee less than 20
    def query6(self):
        print('Get all the Airbnbs with cleaning fee less than $$')
        self.take_city()
        fee = input("Enter max affordable cleaning fee: ")
        fee = int(fee)
        query = {'country_code' : self.country_code, 'city' : self.city, 'cleaning_fee':  { '$lt' : fee} }
        cursor = self.collection.find(query,{'_id':0,'listing_url':1,'cleaning_fee':1}).limit(self.record_count)
        self.print_records(cursor)
        execStats = self.collection.find(query,{'_id':0,'listing_url':1,'cleaning_fee':1}).explain()['executionStats']
        pprint(execStats)

    
    # 7. Get all the new Airbnb. Host since Jan 2018
    def query7(self):
        print('Get all the Airbnbs with Host availanle since year xxxx...')
        self.take_city()
        year = input("Enter the year from which host were available ")
        tm = year + "-01-01T10:53:53.000Z"
        dt = datetime.strptime(tm, "%Y-%m-%dT%H:%M:%S.000Z")
        query = {'country_code' : self.country_code, 'city' : self.city, 
                 'host_since':  {'$gte' : dt} }
        cursor = self.collection.find(query,{'_id':0,'listing_url':1,'host_since':1}).limit(self.record_count)
        self.print_records(cursor)
        execStats = self.collection.find(query,{'_id':0,'listing_url':1,'host_since':1}).explain()['executionStats']
        pprint(execStats)

    
    
    # 8. Only show the hosts with their identity verified & with is location exact
    def query8(self): 
        print('The hosts with their identity verified & with is location exact...')
        self.take_city()
        query = {'host_identity_verified' : "True",'is_location_exact':'True','country_code' : self.country_code, 'city' : self.city }
        print(query)
        cursor = self.collection.find(query,{'_id':0,'listing_url':1,'host_identity_verified':1}).limit(self.record_count)
        self.print_records(cursor)
    

       # 9. Get the Airbnb in SF with Private Room that accommodates 2 people and has 1 Bed and 
       #    1 Bath with price between 60$ - 80$ per night with the flexible cancellation policy 
       #    and instant bookable
       # > db.airbnb_data.distinct('room_type')
       #   [ "Entire home/apt", "Private room", "Shared room" ]
       #

    def query9(self):
        print('The hosts with Private room for 2, 1B 1B in a price range 60$ to 80$...')
        print('Room types are - ')
        print('[ \"Entire home/apt\", \"Private room\", \"Shared room\" ]')
        self.take_city()

        query = {'room_type' : "Private room",'accommodates':2, 'beds' : 1, 'bathrooms':1,
             'country_code' : self.country_code, 'city' : self.city, 'cancellation_policy':'flexible',
             'instant_bookable': 'True', 'price' : {'$gte' : 60 , '$lte' : 80}}
        cursor = self.collection.find(query,{'_id':0,'listing_url':1,'room_type':1,
                                    'price':1,'host_name':1}).limit(self.record_count)
        self.print_records(cursor)
        execStats = self.collection.find(query,{'_id':0,'listing_url':1,'room_type':1,'price':1,'host_name':1}).explain()['executionStats']
        pprint(execStats)

    
    # 10. Find a host which response within an hour with an acceptance rate of more than 80%
    def query10(self):   
        print('The hosts which response within an hour with an acceptance rate of more than 80%...')
        self.take_city()
        query = {'country_code' : self.country_code, 'city' : self.city, 'host_response_time':'within an hour',
            'host_acceptance_rate':{'$gt': 80}}
        cursor = self.collection.find(query,{'_id':0,'listing_url':1,'host_acceptance_rate':1}).limit(self.record_count)
        self.print_records(cursor)

    # 11. Find Hosts which are verified by Email and Phone both and have identity verified
    def query11(self):
        print('The Host which are verified by Email and Phone both and have identity verified...') 
        self.take_city()
        query = {'country_code' : self.country_code, 'city' : self.city, 'host_identity_verified': "True",
         'host_verifications' : {"$all" : ['email','phone']}}
        cursor = self.collection.find(query,{'_id':0,'listing_url':1,'host_identity_verified':1}).limit(self.record_count)
        self.print_records(cursor)
    
    # 12. Find the room URL within the specific zip code    
    def query12(self):
        print('Finding the room URL within the specific zip code..')
        self.take_city()
        zipcode = input("Enter the Zip code:")
        query = {'country_code' : self.country_code, 'city' : self.city, 'zipcode':  zipcode}
        cursor = self.collection.find(query,{'_id':0,'listing_url':1,'zipcode':1}).limit(self.record_count)
        self.print_records(cursor)
        execStats = self.collection.find(query,{'_id':0,'listing_url':1,'zipcode':1}).explain()['executionStats']
        pprint(execStats)
        

    # 13. Find the rooms by Property type (House, Apartment, Cabin, Office.. etc)
    def query13(self):
        print('Finding the room by Property type House, Apartment, Cabin, Office.. etc...')
        self.take_city()
        property_type = input("Enter property type:")
        query = {'country_code' : self.country_code, 'city' : self.city, 'property_type':  property_type}
        cursor = self.collection.find(query,{'_id':0,'listing_url':1}).limit(self.record_count) 
        self.print_records(cursor)
        
    # 14. Find the rooms within a specified price range
    def query14(self):
        print('Finding the rooms within a specified price range...')
        self.take_city()
        low = input("Enter lower boundary of price range:")
        low = int(low)
        high = input("Enter higher boundary of price range:")
        high = int(high)
        query = {'country_code' : self.country_code, 'city' : self.city, 'price' : {'$gt' : low , '$lt' : high}}
        cursor = self.collection.find(query,{'_id':0,'listing_url':1,'price':1}).limit(self.record_count).sort('price')
        self.print_records(cursor)
    
    # 15. Find Host which allow n extra people
    def query15(self):
        print('Find Host which allow n extra people..')
        self.take_city()
        extra_price = int(input("Price for extra person"))
        query = {'country_code' : self.country_code, 'city' : self.city, 'extra_people': {"$lt" : extra_price ,"$gt" : 0}}
        cursor = self.collection.find(query,{'_id':0,'listing_url':1,'extra_people':1}).limit(self.record_count).sort('extra_price')
        self.print_records(cursor)
    
    def query_for_shards(self):
        print('Showing shard distribution..')
        print("self.collection.find({'country_code':{\"$in\":['US','GB','CA','NL']}})")        
        execStats = self.collection.find({'country_code':{"$in":['US','GB','CA','NL']}}).explain()['executionStats']
        pprint(execStats)

def exitcall():
    print('exiting the script')
    exit(1)
    
def print_choices():
    
    print('=============================================================================')
    print('1. List all the Airbnb in San Francisco with cleanliness reviews 10')
    print('2. Count the host listings in San Francisco')
    print('3. Find all the hosts in New York city that respond within an hour')
    print('4. Find what is the maximum number of reviews')
    print('5. Find the minimum deposit ')
    print('6. Get all the Airbnbs in San Francisco with cleaning fee less than 20')
    print('7. Get all the new Airbnb. Host since Jan 2018')
    print('8. Only show the hosts with their identity verified & with is location exact')
    print('9. The hosts with Private room for 2, 1B 1B in a price range 60$ to 80$ ')
    print('10. The hosts which response within an hour with an acceptance rate of more than 80')
    print('11. Find Host which are verified by Email and Phone both and have identity verified')
    print('12. Find the room URL within the specific zip code')
    print('13. Find the rooms by Property type House, Apartment, Cabin, Office.. etc')
    print('14. Find the rooms within a specified price range')
    print('15. Find all rooms which allow extra people under some price')
    print('-1. EXIT')

    

# =============================================================================
#  Execute Queries
# =============================================================================
    
if __name__=="__main__":
    
    airbnb = Airbnb()
    choice = { 
           1 : airbnb.query1,
           2 : airbnb.query2,
           3 : airbnb.query3,
           4 : airbnb.query4,
           5 : airbnb.query5,
           6 : airbnb.query6,
           7 : airbnb.query7,
           8 : airbnb.query8,
           9 : airbnb.query9,
           10 : airbnb.query10,
           11 : airbnb.query11,
           12 : airbnb.query12,
           13 : airbnb.query13,
           14 : airbnb.query14,
           15 : airbnb.query15,
           -1 : exitcall       
        }
    
    print('=============================================================================')
    airbnb.query_for_shards()
    print_choices()
    select = int(input("Please enter the choice:"))
    while select != -1:
        choice[select]()
        print_choices()
        select = int(input("Please enter the choice:"))
    
