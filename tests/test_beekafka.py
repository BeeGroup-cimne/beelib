"""beekafka: la partició 0 també es fixa, i `send_to_kafka` amb `buckets` com a camp reservat del missatge."""
import unittest
from unittest.mock import MagicMock

from tests import _setup  # noqa: F401
from beelib import beekafka


def producer():
    p = beekafka.BeeProducer.__new__(beekafka.BeeProducer)      # sense connectar a Kafka
    p.value_serializer = lambda v: v
    p.key_serializer = lambda v: v
    p.producer = MagicMock()
    return p


class PartitionTests(unittest.TestCase):
    def sent(self, **kw):
        p = producer()
        p.send("topic", value=b"x", **kw)
        return p.producer.produce.call_args.kwargs

    def test_partition_zero_is_fixed(self):
        self.assertEqual(self.sent(partition=0)["partition"], 0)

    def test_any_other_partition_is_fixed(self):
        self.assertEqual(self.sent(partition=3)["partition"], 3)

    def test_no_partition_leaves_the_choice_to_kafka(self):
        self.assertNotIn("partition", self.sent())
        self.assertNotIn("partition", self.sent(partition=None))

    def test_timestamp_is_still_optional(self):
        self.assertNotIn("timestamp", self.sent())
        self.assertEqual(self.sent(timestamp_ms=5)["timestamp"], 5)


class SendToKafkaTests(unittest.TestCase):
    def setUp(self):
        self.producer = MagicMock()

    def message(self):
        return self.producer.send.call_args.kwargs["value"]

    def test_without_buckets_the_message_is_the_one_of_always(self):
        beekafka.send_to_kafka(self.producer, "t", None, [{"id": "a"}], tables=["x"], row_keys=[["id"]])
        self.assertEqual(self.message(), {"data": [{"id": "a"}], "tables": ["x"], "row_keys": [["id"]]})

    def test_buckets_go_in_their_own_field_of_the_message(self):
        beekafka.send_to_kafka(self.producer, "t", None, [{"id": "a"}], buckets=4096, tables=["x"], row_keys=[["id"]])
        self.assertEqual(self.message(), {"data": [{"id": "a"}], "buckets": 4096, "tables": ["x"], "row_keys": [["id"]]})

    def test_key_is_encoded_when_given(self):
        beekafka.send_to_kafka(self.producer, "t", "k", [{"id": "a"}])
        self.assertEqual(self.producer.send.call_args.kwargs["key"], b"k")

    def test_a_row_with_buckets_as_a_key_is_an_error_and_nothing_is_sent(self):
        for kwargs in ({}, {"buckets": 64}):
            with self.assertRaises(ValueError):
                beekafka.send_to_kafka(self.producer, "t", None, [{"id": "a"}, {"id": "b", "buckets": 1}], **kwargs)
        self.producer.send.assert_not_called()

    def test_buckets_must_be_a_positive_integer(self):
        for bad in (0, -1, "64", 1.5, True):
            with self.assertRaises(ValueError, msg=repr(bad)):
                beekafka.send_to_kafka(self.producer, "t", None, [{"id": "a"}], buckets=bad)
        self.producer.send.assert_not_called()

    def test_a_generator_is_not_consumed_by_the_check(self):
        beekafka.send_to_kafka(self.producer, "t", None, ({"id": str(i)} for i in range(3)), tables=["x"])
        self.assertEqual(len(list(self.message()["data"])), 3)

    def test_rows_that_are_not_dicts_are_left_alone(self):
        beekafka.send_to_kafka(self.producer, "t", None, ["a", "b"], tables=["x"])
        self.assertEqual(self.message()["data"], ["a", "b"])


if __name__ == "__main__":
    unittest.main()
