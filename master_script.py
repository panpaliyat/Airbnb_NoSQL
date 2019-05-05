from pymongo import MongoClient

class Airbnb:
    def __init__(self):
        country_code,city,record_count = self.common_input()
        collection = self.make_connection()
        self.country_code = country_code
        self.city = city
        self.record_count = record_count
        self.collection = collection
