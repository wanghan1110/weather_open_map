import requests
from credentials.api_id import api_id
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

import argparse
import atexit
import json
import logging
import random
import time
from apscheduler.schedulers.background import BackgroundScheduler

# - default kafka topic to write to
topic_name = 'weather-analyzer'

# - default kafka broker location
kafka_broker = '127.0.0.1:9092'

# - logging
logging.basicConfig()
logger = logging.getLogger('data_producer')
logger.setLevel(logging.DEBUG)

# - set up scheduler
schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()

def fetch_weather_zipcode(producer,zipcode):
    """
    fetch current weather data from open weather map
    https://openweathermap.org/current
    """
    logger.debug('start to fetch weather info for zipcode=%s',zipcode)
    api_url = 'http://api.openweathermap.org/data/2.5/weather?zip='+zipcode+',us'
    header = {'appid':api_id}
    response = requests.get(api_url,header)
    if response.status_code == 401:
        logger.warn('Invalid API key. status_code=%d',response.status_code)
    elif response.status_code == 404:
        logger.warn('Invalid USA zipcode. status_code=%d',response.status_code)
    elif response.status_code != 200:
        logger.warn('Failed to fetch weather from API. status_code=%d',response.status_code)
    else:
        logger.debug('Retrieved weather data.')
        data=response.json()
        data[u'main'][u'temp'] += random.randint(-5,5)
        data[u'main'][u'humidity'] += random.randint(-5,5)
        data[u'main'][u'pressure'] += random.randint(-5,5)
        main = {'zipcode':zipcode,'name':data['name'],'temp':data[u'main'][u'temp'],'humidity':data[u'main'][u'humidity'],'pressusre':data[u'main'][u'pressure']}
        logger.debug(main)
        producer.send(topic=topic_name, value=main, timestamp_ms=time.time())
        logger.debug('Sent weather data for zipcode %s to Kafka', zipcode)

def shutdown_hook(producer):
    """
    a shutdown hook to be called before the shutdown
    :param producer: instance of a kafka producer
    :return: None
    """
    try:
        logger.info('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s', e.message)

if __name__ == '__main__':
    # - setup command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('zipcode', help='USA zipcode to collect current weather information')
    parser.add_argument('topic_name', help='the kafka topic push to')
    parser.add_argument('kafka_broker', help='the location of the kafka broker')

    # - parse argument
    args = parser.parse_args()
    zipcode = args.zipcode
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    # - instantiate a simple kafka producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    fetch_weather_zipcode(producer,zipcode)

    schedule.add_job(fetch_weather_zipcode, 'interval', [producer, zipcode], seconds=3)

    # - setup proper shutdown hook
    atexit.register(shutdown_hook, producer)

    while True:
        pass

# running command line: python data_producer.py 15213 weather-analyzer localhost:9092
# running Kafka comsumer:
# ./kafka-console-consumer.sh --zookeeper localhost:2181 --topic weather-analyzer --from-beginning
