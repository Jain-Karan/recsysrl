from cassandra.cluster import Cluster
import logging
from airflow.exceptions import AirflowException

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CassandraConnector:
    def __init__(self, contact_points):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect()

    def select_data(self):
        query = "SELECT * FROM fashion_ns.actions"
        result = self.session.execute(query)
        
        data_dict = {}
        
        for row in result:
            data_dict['action'] = row.productlist
        
        if len(data_dict) == 0:
            data_dict['action'] = ''
        
        return data_dict

    def close(self):
        self.cluster.shutdown()


def check_cassandra_main():
    cassandra_connector = CassandraConnector(['cassandra'])
    # Now if there was a visualisation service we could use that to display the results
    data_dict = cassandra_connector.select_data()

    cassandra_connector.close()

    logger.info(f"Data retieved")
    
    return data_dict
