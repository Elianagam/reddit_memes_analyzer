import pika
import pika.exceptions
import time
from common.utils import logger
from common.health_check.utils.connections import connect_retry


class Connection:
    def __init__(self, queue_name='', exchange_name='', bind=False,
                 conn=None, exchange_type='fanout', routing_key='', timeout=15):
        if conn is not None:
            self.connection = conn.connection
            self.channel = conn.channel
        else:
            self.connection = connect_retry(host='rabbitmq',
                                            heartbeat=60*15,
                                            blocked_connection_timeout=60*15)
            if not self.connection:
                logger.error("No connection could be established")

            self.channel = self.connection.channel()
            self.channel.confirm_delivery()

        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.__declare(bind, exchange_type, routing_key)

    def __declare(self, bind, exchange_type, routing_key):
        if self.queue_name != '':
            self.channel.queue_declare(queue=self.queue_name, durable=True)

        if self.exchange_name != '':
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=exchange_type
            )
        if bind:
            # Si es exgange que recibe tiene que crear una anon queue 
            anon_queue = self.channel.queue_declare(queue=f"{self.exchange_name}_{routing_key}", exclusive=False)
            self.queue_name = anon_queue.method.queue
            if routing_key:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key  # consume some msgs
                )
            else:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                )

    def send(self, body, routing_key=None):
        if routing_key == None:
            routing_key = self.queue_name

        sent = False
        while not sent:
            try:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,  # self.queue_name, #
                    body=body,
                    properties=pika.BasicProperties(delivery_mode=2)  # message persistent
                )
                sent = True
            except pika.exceptions.UnroutableError:
                time.sleep(1)

    def recv(self, callback, start_consuming=True, auto_ack=True):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=auto_ack
        )
        if start_consuming:
            self.channel.start_consuming()

    def close(self):
        self.channel.stop_consuming()
        self.connection.close()

    def get_channel(self):
        return self.channel

    def get_queue(self):
        return self.queue_name