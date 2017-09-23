from kafka import KafkaProducer


class KafkaConnector:
    def __init__(self, request_timeout_ms, send_message_timeout_sec):
        self._producer = None
        self._request_timeout_ms = request_timeout_ms
        self._send_message_timeout_sec = send_message_timeout_sec

    def connect(self, hosts):
        try:
            self._producer = KafkaProducer(bootstrap_servers=hosts,
                                            request_timeout_ms=self._request_timeout_ms)
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

