from kafka import KafkaConsumer
import json 
from mstrio import connection
#from mstrio.project_objects.datasets import Dataset
class MstrioLive:
    def __init__(self,base_url,username,password,dataset_name,columns):
        self.base_url = base_url
        self.username = username
        self.pasword = password
        self.dataset_name = dataset_name
        self.columns = columns
        # Create a new dataset
        #self.conn_mgr = connection(base_url=base_url, username=username, password=password)
    
        # self.new_dataset = Dataset.create(
        #     conn_mgr=self.conn_mgr,
        #     name=self.dataset_name,
        #     description="Your Dataset Description",
        #     columns=self.columns
        # )
 
    def parse_message(self,message):
        """
        Parse a JSON message received from Kafka and return it as a dictionary.
        """
        return json.loads(message.decode("utf-8"))


    def update_dataset(self):
        consumer = KafkaConsumer("count", bootstrap_servers=["localhost:9092"])
        # Process the updates and send them to the dataset
        for message in consumer:
            # Parse the message and convert it to a dictionary
            message_dict = self.parse_message(message.value)
            print(message_dict)
            # Add the data to the dataset
            #new_dataset.Dataframe.add_data(message_dict)

            # Save the changes to the dataset
            #new_dataset.Dataframe.commit_data()


base_url = "https://your_microstrategy_server.com/MicroStrategyLibrary/api"
username = "your_username"
password = "your_password"

# Connect to the server
#conn_mgr = ConnectionManager(base_url=base_url, username=username, password=password)

# Create a new dataset

# Create a Kafka consumer to receive updates



columns = [{"name": "Column 1", "dataType": "CharString"}, {"name": "Column 2", "dataType": "Integer"}]

live = MstrioLive(base_url,username,password,"livedata",columns)
live.update_dataset()