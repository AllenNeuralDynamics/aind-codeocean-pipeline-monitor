"""Tests for models module"""

import json
import os
import unittest
from unittest.mock import patch

from codeocean.computation import RunParams
from codeocean.data_asset import AWSS3Target, Target
from pydantic import ValidationError

from aind_codeocean_pipeline_monitor.models import (
    CaptureSettings,
    DocDbSettings,
    PipelineMonitorSettings,
)


class TestDocDbSettings(unittest.TestCase):
    """Tests for DocDbSettings class."""

    @patch.dict(
        os.environ,
        {
            "DOCDB_API_GATEWAY": "example.com",
            "DOCDB_DATABASE": "db",
            "DOCDB_COLLECTION": "coll",
            "RESULTS_BUCKET": "example_bucket",
        },
        clear=True,
    )
    def test_construction(self):
        """Tests settings will pull from env vars"""

        docdb_settings = DocDbSettings()
        self.assertEqual("example.com", docdb_settings.docdb_api_gateway)
        self.assertEqual("db", docdb_settings.docdb_database)
        self.assertEqual("coll", docdb_settings.docdb_collection)
        self.assertEqual("example_bucket", docdb_settings.results_bucket)


class TestsCapturedDataAssetParams(unittest.TestCase):
    """Tests for CapturedDataAssetParams model"""

    def test_construction(self):
        """Basic model construct."""

        model = CaptureSettings(
            tags=["derived, 123456, ecephys"],
            description="some data",
            custom_metadata={"data level": "derived"},
        )
        expected_model_json = {
            "tags": ["derived, 123456, ecephys"],
            "description": "some data",
            "permissions": {
                "everyone": "viewer",
                "groups": [
                    {"group": "AIND Data Administrators", "role": "owner"}
                ],
            },
            "custom_metadata": {"data level": "derived"},
            "data_description_file_name": "data_description.json",
            "process_name_suffix": "processed",
            "process_name_suffix_tz": "UTC",
        }
        self.assertEqual(
            expected_model_json,
            json.loads(model.model_dump_json(exclude_none=True)),
        )

    def test_set_target(self):
        """Test target can be defined"""
        model = CaptureSettings(
            tags=["derived, 123456, ecephys"],
            target=Target(aws=AWSS3Target(bucket="my-bucket", prefix="")),
        )
        expected_model_json = {
            "permissions": {
                "everyone": "viewer",
                "groups": [
                    {"group": "AIND Data Administrators", "role": "owner"}
                ],
            },
            "tags": ["derived, 123456, ecephys"],
            "target": {"aws": {"bucket": "my-bucket", "prefix": ""}},
            "process_name_suffix": "processed",
            "process_name_suffix_tz": "UTC",
        }
        self.assertTrue(
            expected_model_json, model.model_dump_json(exclude_none=True)
        )


class TestsPipelineMonitorSettings(unittest.TestCase):
    """Tests PipelineMonitorSettings model"""

    def test_basic_construct(self):
        """Test basic model constructor"""
        capture_settings = CaptureSettings(
            tags=["derived, 123456, ecephys"],
            custom_metadata={"data level": "derived"},
        )
        run_params = RunParams(pipeline_id="abc-123", version=2)
        settings = PipelineMonitorSettings(
            capture_settings=capture_settings,
            run_params=run_params,
        )
        expected_model_json = {
            "run_params": {"pipeline_id": "abc-123", "version": 2},
            "capture_settings": {
                "tags": ["derived, 123456, ecephys"],
                "custom_metadata": {"data level": "derived"},
                "data_description_file_name": "data_description.json",
                "process_name_suffix": "processed",
                "process_name_suffix_tz": "UTC",
                "permissions": {
                    "everyone": "viewer",
                    "groups": [
                        {"group": "AIND Data Administrators", "role": "owner"}
                    ],
                },
            },
            "computation_polling_interval": 180,
            "data_asset_ready_polling_interval": 10,
        }
        self.assertEqual(
            expected_model_json,
            json.loads(settings.model_dump_json(exclude_none=True)),
        )

    def test_basic_construct_job_type(self):
        """Test basic model constructor with job_type"""
        capture_settings = CaptureSettings(
            tags=["derived, 123456, ecephys"],
            custom_metadata={"data level": "derived"},
        )
        run_params = RunParams(pipeline_id="abc-123", version=2)
        settings1 = PipelineMonitorSettings(
            capture_settings=capture_settings,
            run_params=run_params,
        )
        settings2 = PipelineMonitorSettings(
            capture_settings=capture_settings,
            run_params=run_params,
            job_type="ecephys",
        )
        self.assertIsNone(settings1.job_type)
        self.assertEqual("ecephys", settings2.job_type)

    def test_validator_success(self):
        """Tests validation success"""
        capture_settings = CaptureSettings(
            tags=["derived, 123456, ecephys"],
            custom_metadata={"data level": "derived"},
        )
        run_params = RunParams(pipeline_id="abc-123", version=2)
        settings = PipelineMonitorSettings(
            capture_settings=capture_settings,
            run_params=run_params,
            computation_polling_interval=100,
            computation_timeout=200,
            data_asset_ready_polling_interval=120,
            data_asset_ready_timeout=2000,
        )
        self.assertEqual(float(2000), settings.data_asset_ready_timeout)
        self.assertEqual(float(200), settings.computation_timeout)

    def test_validator_fail(self):
        """Tests validation fails if computation timeout less than polling
        interval"""
        capture_settings = CaptureSettings(
            tags=["derived, 123456, ecephys"],
            custom_metadata={"data level": "derived"},
        )
        run_params = RunParams(pipeline_id="abc-123", version=2)
        with self.assertRaises(ValidationError) as e:
            PipelineMonitorSettings(
                capture_settings=capture_settings,
                run_params=run_params,
                computation_polling_interval=100,
                computation_timeout=90,
                data_asset_ready_polling_interval=120,
                data_asset_ready_timeout=120,
            )
        errors = json.loads(e.exception.json())
        self.assertEqual(2, len(errors))
        self.assertIn("computation_timeout", errors[0]["msg"])
        self.assertIn("data_asset_ready_timeout", errors[1]["msg"])


if __name__ == "__main__":
    unittest.main()
