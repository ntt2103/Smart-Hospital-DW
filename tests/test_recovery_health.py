import unittest
from unittest.mock import Mock

from operations.phase5_recovery import clone_database_policy, compare_schema, health_request, http_health


class RecoveryHealthTests(unittest.TestCase):
    def test_http200_with_unhealthy_scheduler_is_not_accepted(self):
        body={'metadatabase':{'status':'healthy'},'scheduler':{'status':'unhealthy'},
              'dag_processor':{'status':'healthy'}}
        states=http_health('airflow-api',200,body,{'airflow-api','airflow-scheduler','airflow-dag-processor'})
        self.assertTrue(states['airflow-api']['ready'])
        self.assertFalse(states['airflow-scheduler']['ready'])
        self.assertTrue(states['airflow-dag-processor']['ready'])

    def test_stopped_workers_and_undeployed_triggerer_are_not_required(self):
        body={'metadatabase':{'status':'healthy'},'scheduler':{'status':'unhealthy'},
              'triggerer':{'status':'unhealthy'},'dag_processor':None}
        self.assertEqual(set(http_health('airflow-api',200,body,{'airflow-api'})),{'airflow-api'})
        self.assertTrue(http_health('airflow-api',200,body,{'airflow-api'})['airflow-api']['ready'])

    def test_requested_missing_dag_processor_is_unhealthy(self):
        body={'metadatabase':{'status':'healthy'},'scheduler':{'status':'healthy'},'dag_processor':None}
        states=http_health('airflow-api',200,body,{'airflow-api','airflow-dag-processor'})
        self.assertFalse(states['airflow-dag-processor']['ready'])

    def test_metabase_json_and_http_status_must_both_be_healthy(self):
        self.assertTrue(http_health('metabase',200,{'status':'ok'},set())['metabase']['ready'])
        self.assertFalse(http_health('metabase',200,{'status':'initializing'},set())['metabase']['ready'])
        self.assertFalse(http_health('metabase',503,{'status':'ok'},set())['metabase']['ready'])

    def test_booking_has_its_own_explicit_health_status(self):
        self.assertTrue(http_health('booking',200,{'status':'running'},set())['booking']['ready'])
        self.assertFalse(http_health('booking',200,{'status':'stopping'},set())['booking']['ready'])

    def test_internal_booking_probe_uses_allowed_host_without_changing_target(self):
        request=health_request('booking','http://booking:8080/health')
        self.assertEqual(request.full_url,'http://booking:8080/health')
        self.assertEqual(request.get_header('Host'),'localhost:8080')
        self.assertIsNone(health_request('metabase','http://metabase:3000/api/health').get_header('Host'))

    def test_database_grant_clone_refuses_existing_business_names(self):
        connection=Mock()
        for name in ('hospital_oltp','hospital_dwh','airflow_meta','metabase_app'):
            with self.subTest(database=name),self.assertRaises(ValueError):
                clone_database_policy(connection,name,{'owner':'postgres','acl':[]})
        connection.execute.assert_not_called()

    def test_equivalent_restore_deparser_requires_matching_archive_schema(self):
        original={'columns':{'count':3,'sha256':'dropped-column-holes'},
                  'constraints':{'count':1,'sha256':'array-cast'},
                  'owners':{'count':1,'sha256':'same-owner'}}
        restored={'columns':{'count':3,'sha256':'compacted-logical-columns'},
                  'constraints':{'count':1,'sha256':'element-casts'},
                  'owners':{'count':1,'sha256':'same-owner'}}
        self.assertFalse(compare_schema(original,restored)['matches'])
        result=compare_schema(original,restored,restored)
        self.assertTrue(result['matches'])
        self.assertFalse(result['raw_captured_schema_matches'])
        self.assertTrue(result['archive_schema_reference_matches'])

    def test_archive_reference_cannot_override_original_ownership_or_grants(self):
        original={'owners':{'count':1,'sha256':'original-owner'},
                  'relation_acl':{'count':2,'sha256':'original-grants'}}
        changed={'owners':{'count':1,'sha256':'wrong-owner'},
                 'relation_acl':{'count':2,'sha256':'wrong-grants'}}
        self.assertFalse(compare_schema(original,changed,changed)['matches'])

    def test_archive_reference_cannot_hide_missing_columns(self):
        original={'columns':{'count':3,'sha256':'three-columns'}}
        changed={'columns':{'count':2,'sha256':'two-columns'}}
        self.assertFalse(compare_schema(original,changed,changed)['matches'])

    def test_different_archive_reference_is_not_accepted(self):
        original={'views':{'count':2,'sha256':'old-cast-form'}}
        restored={'views':{'count':2,'sha256':'different-expression'}}
        reference={'views':{'count':2,'sha256':'correct-archive-expression'}}
        self.assertFalse(compare_schema(original,restored,reference)['matches'])


if __name__=='__main__': unittest.main()
