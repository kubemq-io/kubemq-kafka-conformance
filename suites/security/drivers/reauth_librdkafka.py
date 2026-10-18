"""librdkafka (confluent-kafka-python) producer for the security track's re-authentication row.

Usage: reauth_librdkafka.py <bootstrap> <topic> <sasl mechanism> <seconds>
Sends one record every 2 seconds for <seconds> as alice over SASL_PLAINTEXT and prints one JSON
line: records acknowledged, records failed, and the librdkafka version. librdkafka re-authenticates
(KIP-368) when the broker gives the session a lifetime.
"""
import json
import sys
import time

import confluent_kafka

bootstrap, topic, mechanism, seconds = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
counts = {"acked": 0, "failed": 0}


def delivered(err, _msg):
    counts["failed" if err else "acked"] += 1


producer = confluent_kafka.Producer({
    "bootstrap.servers": bootstrap,
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": mechanism,
    "sasl.username": "alice",
    "sasl.password": "alice-secret",
    "acks": "all",
})
end = time.time() + seconds
i = 0
while time.time() < end:
    producer.produce(topic, str(i).encode(), callback=delivered)
    producer.flush(15)
    i += 1
    time.sleep(2)
print(json.dumps({"acked": counts["acked"], "failed": counts["failed"], "librdkafka": confluent_kafka.libversion()[0]}))
