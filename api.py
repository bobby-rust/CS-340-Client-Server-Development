from pymongo import MongoClient

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, username, password):
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections.
        # This is hard-wired to use the aac database, the 
        # animals collection, and the aac user.
        # Definitions of the connection string variables are
        # unique to the individual Apporto environment.
        #
        # You must edit the connection variables below to reflect
        # your own instance of MongoDB!
        #
        # Connection Variables
        #
        USER = username 
        PASS = password 
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 31811
        DB = 'AAC'
        COL = 'animals'
        #
        # Initialize Connection
        #
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT), connect=False)
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]

    # Method to implement the C in CRUD.
    def create(self, data):
        if data is not None:
            self.database.animals.insert_one(data)  # data should be dictionary            
            return True
        else:
            raise Exception("Nothing to save, because data parameter is empty")
            return False

    # Method to implement the R in CRUD.
    def read(self, data):
        if data is not None:
            result = list(self.database.animals.find(data)) # cast cursor to list
            return result
        else:
            raise Exception("Nothing to find because data parameter is empty")

    def update(self, data):
        # Extract and validate the arguments 
        op = data["op"]
        if not op:
            # Print a useful error message to help guide the api consumer
            print("Please pass the update operation you would like to perform in the 'op' key. Valid values are 'one' or 'many'")
            return

        if data["op"] != "one" and data["op"] != "many":
            # Print a useful error message to help guide the api consumer
            print("Failed to execute delete query. Reason: invalid value for 'op' key. Valid values are 'one' or 'many'")
            return

        pattern = data["match"]
        if not pattern:
            print("Please pass a 'match' field containing the documents you would like to update. Ex: { 'name': 'Fluffy' }")

        new_values = data["new_values"]
        if not new_values:
            print("Please pass a 'new_values' field containing the updated values. Ex: { 'name': 'Marshmallow' }") 

        
       
        if op == "one":
            result = self.database.animals.update_one(pattern, { "$set": new_values })
        else:
            result = self.database.animals.update_many(pattern, { "$set": new_values })
    
        return result.modified_count

      
    def delete(self, data):
        # Extract and validate the database operation
        op = data["op"]
        if not op:
            # Print a useful error message to help guide the api consumer
            print("Please pass the update operation you would like to perform in the 'op' key. Valid values are 'one' or 'many'")
            return

        if op != "one" and op != "many":
            print("Failed to execute DELETE query. Reason: invalid value for 'op' key. Valid values are 'one' or 'many'")
        
        # Extract and ensure the match field exists
        pattern = data["match"]
        if not pattern:
            print("Please pass the 'match' field containing the pattern(s) of the documents you would like to delete.")

        if op == "one":
            result = self.database.animals.delete_one(pattern)
        else:
            result = self.database.animals.delete_many(pattern)

        return result.deleted_count
