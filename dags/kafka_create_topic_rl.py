from confluent_kafka.admin import AdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

admin_config = {
    'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094',
    'client.id': 'kafka_admin_client'
}

admin_client = AdminClient(admin_config)

def kafka_create_topic_main():
    """Checks if the topics exists or not. If not, creates the topics."""
    topic_names = ['fashion_state','fashion_action']

    existing_topics = admin_client.list_topics().topics
    
    message=''
    for topic_name in topic_names:
        if topic_name in existing_topics:
            message+=topic_name+"Exists"
        
        # Create the new topic
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=3)
        admin_client.create_topics([new_topic])
        message+=topic_name+"Created"
        
    return message


if __name__ == "__main__":
    result = kafka_create_topic_main()
    logger.info(result)