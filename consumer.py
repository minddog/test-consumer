#!/usr/bin/env python
import logging, time, datetime, os
import gevent
from pagerduty import pagerduty_event

from kafka.client import (KafkaClient, KafkaUnavailableError,
                          ConnectionError, LeaderNotAvailableError, FailedPayloadsError)
from kafka.common import BufferUnderflowError
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
from gevent.monkey import patch_all; patch_all()
import signal

LOG_FORMAT='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s'
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_FORMAT)
logger = logging.getLogger("test-consumer")
logger.setLevel(logging.DEBUG)
producer_host = "{}:{}".format(os.getenv('KAFKA_PORT_9092_TCP_ADDR'),
                               os.getenv('KAFKA_PORT_9092_TCP_PORT'))

CONNECT_MAX_RETRIES = 10
CONNECT_BACKOFF_SECONDS = 10
CONSUMER_MAX_RETRIES = 10
CONSUMER_BACKOFF_SECONDS = 10
FLAP_THRESHOLD = 10
TEST_CHANNEL = 'atc-end2end'
BUFFERUNDERFLOW_BACKOFF_SECONDS = 2

def connect():
    client = None
    retry = 0

    def _connect():
        try:
            return KafkaClient(producer_host)
        except ConnectionError, e:
            logger.warn("Connection Error, failed to connect: {}".format(e.message))
        except KafkaUnavailableError, e:
            logger.warn("Kafka Unavailable, can not connect: {}".format(e.message))

        return None

    while not client:
        client = _connect()
        if not client:
            retry += 1
            if retry >= CONNECT_MAX_RETRIES:
                logger.warn("Max attempts ({}) to connect exhausted, backing off for {} seconds and resetting retry counter to zero".format(retry, CONNECT_BACKOFF_SECONDS))
                gevent.sleep(CONNECT_BACKOFF_SECONDS)
                retry = 0
            logger.warning("Retrying connection to Kafka: {}".format(retry))

    return client

def get_consumer(client, group, topic):
    retry = 0
    consumer = None

    def _get_consumer():
        try:
            return SimpleConsumer(client, group, topic)
        except LeaderNotAvailableError, e:
            logger.warn("Unable to get consumer, retrying: {}".format(e))

    while not consumer:
        consumer = _get_consumer()
        if not consumer:
            retry += 1
            if retry > CONSUMER_MAX_RETRIES:
                logger.warn("Backing off fetching consumer for {}".format(CONSUMER_BACKOFF_SECONDS))
                time.sleep(CONSUMER_BACKOFF_SECONDS)
                retry = 0

    return consumer

class Producer(object):

    def run(self):
        logger.info("Starting Producer")
        self.connect()

        while True:
            current_time = time.strftime("%a, %d %b %Y %H:%M:%S UTC", time.gmtime())
            gevent.spawn(self.send_message, TEST_CHANNEL, "{}".format(current_time))
            time.sleep(1)

    def send_message(self, topic, message):
        try:
            self.producer.send_messages(topic, message)
        except LeaderNotAvailableError, e:
            # try again
            self.producer.send_messages(topic, message)
        except KafkaUnavailableError, e:
            logger.info("Kafka Unavailable, Reconnecting: {}".format(e))
            self.connect()
            self.send_message(topic, message)


    def connect(self):
        self.client = connect()
        self.producer = SimpleProducer(self.client)

class Consumer(object):
    last_time_capture = None
    flapping = False

    def run(self):
        logger.info("Starting Consumer")
        self.connect()

        while True:
            self.read_messages(self.consumer)
            time.sleep(1)


    def read_messages(self, consumer):
        try:
            messages = consumer.get_messages(count=100)
            for item in messages:
                message = item.message
                current_time = datetime.datetime.strptime(message.value, "%a, %d %b %Y %H:%M:%S %Z")
                logger.info("Current_time is {}".format(message.value))
            self.last_time_capture = current_time
        except KeyError, e:
            logger.info("KeyError: {}".format(e))
            time.sleep(BUFFERUNDERFLOW_BACKOFF_SECONDS)
            self.connect()
        except BufferUnderflowError, e:
            logger.info("BufferUnderflow, backing off {} seconds".format(BUFFERUNDERFLOW_BACKOFF_SECONDS))
            time.sleep(BUFFERUNDERFLOW_BACKOFF_SECONDS)
        except KafkaUnavailableError, e:
            logger.info("Kafka went away, reconnecting: {}".format(e))
            self.connect()
        except FailedPayloadsError, e:
            logger.info("PayloadError, retrying: {}".format(e))
            self.connect()
        except ConnectionError, e:
            logger.info("Kafka went away, reconnecting: {}".format(e))
            self.connect()


    def connect(self):
        self.client = connect()
        self.consumer = get_consumer(self.client, "end2end-test-group", TEST_CHANNEL)

    def check_time(self, current_time):
        if not self.last_time_capture:
            self.last_time_capture = current_time

        time_difference = current_time - self.last_time_capture
        logger.error("TIME DIFFERENCE: {}".format(time_difference))
        if self.flapping == False and time_difference.seconds > FLAP_THRESHOLD:
            self.flapping = True
            pagerduty_event(event_type='trigger', incident_key='kafka',
                            description="Kafka is delayed by {} seconds".format(time_difference.seconds))
            logger.error("KAFKA UNAVAILABLE RUN FOR WATER, PUT OUT FIRE!!!! {}".format(self.last_time_capture))
        elif self.flapping == True and time_difference.seconds < FLAP_THRESHOLD:
            self.flapping = False
            pagerduty_event(event_type='resolve', incident_key='kafka',
                            description="Kafka delays are under normal threshold: {} seconds".format(time_difference.seconds))

    def background_check_time(self):
        logger.info("Background check enabled")
        while True:
            current_time = datetime.datetime.utcnow()
            logger.info("Background check running: {}".format(current_time))
            self.check_time(current_time)
            time.sleep(1)

def main():
    consumer = Consumer()
    producer = Producer()
    producer_event, consumer_event = gevent.spawn(producer.run), gevent.spawn(consumer.run)
    consumer_checker_event = gevent.spawn(consumer.background_check_time)
    [gevent.signal(signal.SIGINT, gevent.kill, event) for event in producer_event, consumer_event, consumer_checker_event]
    [gevent.signal(signal.SIGKILL, gevent.kill, event) for event in producer_event, consumer_event, consumer_checker_event]
    gevent.joinall((producer_event, consumer_event, consumer_checker_event))

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.DEBUG
        )
    main()
