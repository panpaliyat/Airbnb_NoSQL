# =============================================================================
#  load libraries
# =============================================================================

from datetime import datetime
from pymongo import MongoClient

class Airbnb:
    def __init__(self):
        country_code,city,record_count = self.common_input()
        collection = self.make_connection()
        self.country_code = country_code
        self.city = city
        self.record_count = record_count
        self.collection = collection
       
    def common_input(self):
        country_code = input("Enter the country code: ")
        city = input("Enter the city (e.g. San Francisco): ")
        record_count = int(input("Number of records to output: "))
        return country_code,city,record_count

    def make_connection(self):
        client = MongoClient(port=27017)
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
        query = {'fields.country_code' : self.country_code, 'fields.city' : self.city, 
                 'fields.review_scores_cleanliness':10 }
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1,'fields.review_scores_cleanliness':1}).limit(self.record_count)
        print_records(cursor)
    
    # 2. Count the host listings in San Francisco
    def query2(self):    
        print("Total number of host listing in San Francisco are ..... ")
        query = {'fields.country_code' : self.country_code, 'fields.city' : self.city}
        cursor = self.collection.find(query).distinct('fields.listing_url')
        print("Records found:",len(cursor))
        

    # 3. Find all the hosts in New York city that respond within an hour
    def query3(self):    
        print("Listing all Airbnbs where hosts respond within an hour ..... ")
        query = {'fields.country_code' : self.country_code, 'fields.city' : self.city, 'fields.host_response_time':'within an hour' }
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1,'fields.host_name':1}).limit(self.record_count)
        self.print_records(cursor)   

    # 4. Find Airbnb with the maximum number of reviews
    def query4(self):    
        print("Listing all Airbnbs with maximum number of reviews ..... ")
        cursor = self.collection.aggregate([{'$group':{'_id':'fields.number_of_reviews','maxReviews': { '$max': '$fields.number_of_reviews'}}}])
        for document in cursor:
            print("The Airbnb with the maximum number of reviews is ", document)

    # 5. Find the minimum deposit 
    def query5(self):    
        print("Listing all Airbnbs which allow minimum deposit ..... ")
        cursor = self.collection.aggregate([{'$group':{'_id':'fields.security_deposit','minDeposit': { '$min': '$fields.security_deposit' }}}])
        for document in cursor:
            print("The Airbnb with the minimum deposit is ", document)


    # 6. Get all the Airbnbs in San Francisco with cleaning fee less than 20
    def query6(self):
        fee = input("Enter max affordable cleaning fee: ")
        fee = int(fee)
        query = {'fields.country_code' : self.country_code, 'fields.city' : self.city, 'fields.cleaning_fee':  { '$lt' : fee} }
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1,'fields.cleaning_fee':1}).limit(self.record_count)
        self.print_records(cursor)
    
    # 7. Get all the new Airbnb. Host since Jan 2018
    def query7(self):
        dt = datetime.strptime('2018-01-01','%Y-%m-%d')
        #date_ = input("Enter date to find all airbnbs since that date")
        query = {'fields.country_code' : self.country_code, 'fields.city' : self.city, 
                 'fields.host_since':  {'$gte' : dt} }
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1,'fields.host_since':1}).limit(self.record_count)
        self.print_records(cursor)
    
    
    # 8. Only show the hosts with their identity verified & with is location exact
    def query8(self):    
        query = {'fields.host_identity_verified' : "True",'fields.is_location_exact':'True','fields.country_code' : self.country_code, 'fields.city' : self.city }
        print(query)
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1,'fields.host_identity_verified':1}).limit(self.record_count)
        self.print_records(cursor)
    

       # 9. Get the Airbnb in SF with Private Room that accommodates 2 people and has 1 Bed and 
       #    1 Bath with price between 60$ - 80$ per night with the flexible cancellation policy 
       #    and instant bookable
       # > db.airbnb_data.distinct('fields.room_type')
       #   [ "Entire home/apt", "Private room", "Shared room" ]
       #

    def query9(self):    
        query = {'fields.room_type' : "Private room",'fields.accommodates':2, 'fields.beds' : 1, 'fields.bathrooms':1,
             'fields.country_code' : self.country_code, 'fields.city' : self.city, 'fields.cancellation_policy':'flexible',
             'fields.instant_bookable': 'True', 'fields.price' : {'$gte' : 60 , '$lte' : 80}}
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1,'fields.room_type':1,
                                    'fields.price':1,'fields.host_name':1}).limit(self.record_count)
        self.print_records(cursor)
    
    # 10. Find a host which response within an hour with an acceptance rate of more than 80%
    def query10(self):    
        query = {'fields.country_code' : self.country_code, 'fields.city' : self.city, 'fields.host_response_time':'within an hour',
            'fields.host_acceptance_rate':{'$gt': 80}}
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1,'fields.host_acceptance_rate':1}).limit(self.record_count)
        self.print_records(cursor)

    # 11. Find Host which are verified by Email and Phone both and have identity verified
    def query11(self):
        verified_type = input("Verified Type")
        query = {'fields.country_code' : self.country_code, 'fields.city' : self.city, 'fields.host_identity_verified': "True",
         'fields.host_verifications' : verified_type}
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1,'fields.host_identity_verified':1}).limit(self.record_count)
        self.print_records(cursor)
    
    # 12. Find the room URL within the specific zip code    
    def query12(self):
        zipcode = input("Enter the Zip code")
        query = {'fields.country_code' : self.country_code, 'fields.city' : self.city, 'fields.zipcode':  zipcode}
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1,'fields.zipcode':1}).limit(self.record_count)
        self.print_records(cursor)


    # 13. Find the rooms by Property type (House, Apartment, Cabin, Office.. etc)
    def query13(self):
        property_type = input("Enter property type")
        query = {'fields.country_code' : self.country_code, 'fields.city' : self.city, 'fields.property_type':  property_type}
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1}).limit(self.record_count) 
        self.print_records(cursor)
        
    # 14. Find the rooms within a specified price range
    def query14(self):
        low = input("Enter lower boundary of price range ")
        low = int(low)
        high = input("Enter higher boundary of price range")
        high = int(high)
        query = {'fields.country_code' : self.country_code, 'fields.city' : self.city, 'fields.price' : {'$gt' : low , '$lt' : high}}
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1,'fields.price':1}).limit(self.record_count).sort('fields.price')
        self.print_records(cursor)
    
    # 15. Find Host which allow n extra people
    def query15(self):
        extra_price = int(input("Price for extra person"))
        query = {'fields.country_code' : self.country_code, 'fields.city' : self.city, 'fields.extra_people': {"$lt" : extra_price ,"$gt" : 0}}
        cursor = self.collection.find(query,{'_id':0,'fields.listing_url':1,'fields.extra_people':1}).limit(self.record_count).sort('fields.extra_price')
        self.print_records(cursor)

# =============================================================================
#  Execute Queries
# =============================================================================
    
if __name__=="__main__":
    airbnb = Airbnb()
    airbnb.query1()
    airbnb.query2()
    airbnb.query3()
    airbnb.query4()
    airbnb.query5()
    airbnb.query6()
    airbnb.query7()
    airbnb.query8()
    airbnb.query9()
    airbnb.query10()
    airbnb.query11()
    airbnb.query12()
    airbnb.query13()
    airbnb.query14()
    airbnb.query15()
    
    
    
    
