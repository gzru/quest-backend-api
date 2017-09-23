from kafka import KafkaProducer


class KafkaConnector:
    def __init__(self):
        self._producer = None
        self._send_message_timeout_sec = 1

    def connect(self, hosts):
        try:
            self._producer = KafkaProducer(bootstrap_servers=hosts,
                                            request_timeout_ms=60000)
        except Exception as ex:
            # log error
            return False
        return True

    def put_message(self, topic, message):
        future = self._producer.send(topic, message)
        try:
            metadata = future.get(timeout=self._send_message_timeout_sec)
        except Exception as ex:
            # log error
            return False
        return True

