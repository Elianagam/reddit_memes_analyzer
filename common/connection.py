import pika
import pika.exceptions
import time
import logging


class Connection:
    def __init__(self, queue_name='', exchange_name='', bind=False,
                 conn=None, exchange_type='fanout', routing_key='', timeout=15):
        if conn is not None:
            self.connection = conn.connection
            self.channel = conn.channel
        else:
            time.sleep(timeout)
            connected = False
            while not connected:
                try:
                    self.connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
                    connected=True

                except pika.exceptions.AMQPConnectionError:
                    logging.info("Rabbitmq not conected yet")
                    time.sleep(1)

            self.channel = self.connection.channel()

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
            anon_queue = self.channel.queue_declare(queue='', exclusive=True)
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

        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=routing_key,  # self.queue_name, #
            body=body,
            properties=pika.BasicProperties(delivery_mode=2)  # message persistent
        )

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
