from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka.errors import KafkaError

import argparse
import atexit
import json
import logging

# - default kafka topic to read from
topic_name = 'weather-analyzer'

# - default kafka broker location
kafka_broker = '127.0.0.1:9092'

# - default cassandra nodes to connect
contact_points = ['127.0.0.1:9042']

# - default keyspace to use
key_space = 'weather'

# - default table to use
data_table = 'weather'

logging.basicConfig()
logger = logging.getLogger('data-storage')
logger.setLevel(logging.DEBUG)

def persist_data(weather_data, cassandra_session):
    """
    persist weather data into cassandra
    :param weather_data:
    the weather_data looks like this:
    {
        "humidity": 43, 
        "pressusre": 1021, 
        "zipcode": "15213", 
        "temp": 296.01, 
        "name": "Pittsburgh"
        "time_stamp":"2017-07-09 23:45:56"
        }
    """
    try:
        logger.debug('Start to persist data to cassandra')
        parsed = json.loads(weather_data)
        zipcode = parsed['zipcode']
        name = parsed['name']
        time_stamp = parsed['time_stamp']
        humidity = int(parsed['humidity'])
        pressure = int(parsed['pressure'])
        temperature = float(parsed['temp'])
        statement = "INSERT INTO %s (zipcode, time_stamp, name, temperature, pressure, humidity) VALUES ('%s', '%s', '%s',%f, %f, %f)" % (data_table, zipcode, time_stamp, name, temperature, pressure, humidity)
        cassandra_session.execute(statement)
        logger.info('Persist data to cassandra for zipcode: %s, time_stamp: %s, name: %s, temperature: %f, pressure: %f, humidity: %f' % (zipcode, time_stamp, name, temperature, pressure, humidity))
    except Exception:
        logger.error('Failed to persist data to cassandra %s', weather_data)


def shutdown_hook(consumer, session):
    """
    a shutdown hook to be called before the shutdown
    :param consumer: instance of a kafka consumer
    :param session: instance of a cassandra session
    :return: None
    """
    try:
        logger.info('Closing Kafka Consumer')
        consumer.close()
        logger.info('Kafka Consumer closed')
        logger.info('Closing Cassandra Session')
        session.shutdown()
        logger.info('Cassandra Session closed')
    except KafkaError as kafka_error:
        logger.warn('Failed to close Kafka Consumer, caused by: %s', kafka_error.message)
    finally:
        logger.info('Existing program')

if __name__ == '__main__':
    # - setup command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='the kafka topic to subscribe from')
    parser.add_argument('kafka_broker', help='the location of the kafka broker')
    parser.add_argument('contact_points', help='the contact points for cassandra')
    parser.add_argument('key_space', help='the keyspace to use in cassandra')
    parser.add_argument('data_table', help='the data table to use')
    

    # - parse arguments
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    key_space = args.key_space
    data_table = args.data_table
    contact_points = ['127.0.0.1']

    # - initiate a simple kafka consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_broker
    )

    # - initiate a cassandra session
    cassandra_cluster = Cluster(
        contact_points=contact_points
    )
    session = cassandra_cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = 'true'" % key_space)
    session.set_keyspace(key_space)
    session.execute("CREATE TABLE IF NOT EXISTS %s (zipcode text, name text, time_stamp timestamp, temperature float, pressure float, humidity float, PRIMARY KEY (zipcode,time_stamp))" % data_table)

    for msg in consumer:
        persist_data(msg.value, session)
        # session.execute("SE")

    # - setup proper shutdown hook
    atexit.register(shutdown_hook, consumer, session)

# running command:
# python data_storage.py weather-analyzer 127.0.0.1:9092 localhost weather weather
