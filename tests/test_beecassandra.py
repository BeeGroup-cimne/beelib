"""beecassandra: `__create_table__` amb el comentari opcional de la taula."""
import unittest
from unittest.mock import MagicMock

from tests import _setup  # noqa: F401
from beelib import beecassandra

OPTIONS = {
    'partition_rows': {'rows': ['pk'], 'types': ['int']},
    'sort_row': {'rows': ['id'], 'types': ['text']},
    'columns': {'info': ['all']},
}


def create(options):
    session = MagicMock()
    beecassandra.__create_table__(session, "ks.t", options)
    return " ".join(session.execute.call_args.args[0].split())


class CreateTableTests(unittest.TestCase):
    def test_without_comment_the_statement_is_the_one_of_always(self):
        self.assertTrue(create(OPTIONS).endswith("WITH CLUSTERING ORDER BY (id DESC);"))
        self.assertNotIn("comment", create(OPTIONS))

    def test_empty_comment_is_ignored(self):
        self.assertNotIn("comment", create({**OPTIONS, "comment": ""}))

    def test_comment_is_added_to_the_table(self):
        self.assertTrue(create({**OPTIONS, "comment": "buckets=4096"})
                        .endswith("WITH CLUSTERING ORDER BY (id DESC) AND comment = 'buckets=4096';"))

    def test_a_single_quote_in_the_comment_is_escaped(self):
        self.assertIn("comment = 'it''s';", create({**OPTIONS, "comment": "it's"}))

    def test_primary_key_and_columns_do_not_change(self):
        query = create({**OPTIONS, "comment": "x"})
        self.assertIn("pk int,id text, info map<text, text>", query)
        self.assertIn("PRIMARY KEY ((pk), id)", query)


if __name__ == "__main__":
    unittest.main()
