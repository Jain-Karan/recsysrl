from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster
import time
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class CassandraConnector:
    def __init__(self, contact_points):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect()
        self.create_keyspace()
        self.create_table()

    def create_keyspace(self):
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS fashion_ns
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)

    def create_table(self):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS fashion_ns.actions (
                productlist text PRIMARY KEY
            )
        """)
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS fashion_ns.states (
                state text PRIMARY KEY
            )
        """)

    def insert_data_action(self, productlist):
        self.session.execute("""
            INSERT INTO fashion_ns.actions (productlist)
            VALUES (%s)
        """, (json.dumps(productlist),))

    def insert_data_state(self, state):
        self.session.execute("""
            INSERT INTO fashion_ns.states (state)
            VALUES (%s)
        """, (json.dumps(state),))

    def shutdown(self):
        self.cluster.shutdown()


def fetch_and_insert_messages(kafka_config, cassandra_connector, topic, run_duration_secs):
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])

    start_time = time.time()
    try:
        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time >= run_duration_secs:
                break
            
            msg = consumer.poll(1.0)
            print("Polling done...........")
            if msg is None:
                print("No messages to import............")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info('Reached end of partition')
                else:
                    logger.error('Error: {}'.format(msg.error()))
            else:
                msg_topic=msg.value().decode('utf-8')
                print(f'Received Data in topic {topic}: {msg_topic}............')
                if topic=="fashion_action":
                    cassandra_connector.insert_data_action(msg_topic)
                else:
                    cassandra_connector.insert_data_state(msg_topic)                
                logger.info(f'Inserted Data in topic {topic}: {msg_topic}...........')
                            
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt. Closing consumer.")
    finally:
        consumer.close()


def kafka_consumer_cassandra_main():
    cassandra_connector = CassandraConnector(['cassandra'])

    cassandra_connector.create_keyspace()
    cassandra_connector.create_table()

    kafka_config = {
        'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094',
        'group.id': 'cassandra_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    run_duration_secs = 10
    for topic in ["fashion_action","fashion_state"]:
        fetch_and_insert_messages(kafka_config, cassandra_connector, topic, run_duration_secs)

    cassandra_connector.shutdown()


kafka_consumer_cassandra_main()
