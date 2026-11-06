"""Transient streaming failures must not leave startup with half a dashboard."""
import unittest
from unittest.mock import Mock, patch
from analytics.provision import query_card


class DashboardStartupTests(unittest.TestCase):
    @patch('analytics.provision.time.sleep')
    def test_empty_response_is_retried(self, sleep):
        client = Mock()
        expected = {'data': {'rows': [[1]]}}
        client.request.side_effect = [None, expected]
        self.assertEqual(query_card(client, 'card/1/query', {}), expected)
        self.assertEqual(client.request.call_count, 2)

    @patch('analytics.provision.time.sleep')
    def test_repeated_empty_response_fails_explicitly(self, sleep):
        client = Mock()
        client.request.return_value = None
        with self.assertRaisesRegex(RuntimeError, 'No completed Metabase'):
            query_card(client, 'card/1/query', {})
        self.assertEqual(client.request.call_count, 5)

    def test_query_error_is_preserved_for_caller(self):
        client = Mock()
        client.request.return_value = {'error': 'invalid SQL'}
        self.assertEqual(query_card(client, 'card/1/query', {}), {'error': 'invalid SQL'})
        self.assertEqual(client.request.call_count, 1)
