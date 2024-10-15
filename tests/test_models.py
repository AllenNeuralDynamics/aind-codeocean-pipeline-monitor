"""Tests for models module"""

import json
import unittest

from codeocean.computation import RunParams
from codeocean.data_asset import AWSS3Target, Target

from aind_codeocean_pipeline_monitor.models import (
    CaptureSettings,
    PipelineMonitorSettings,
)


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
            "permissions": {"everyone": "viewer"},
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
            "permissions": {"everyone": "viewer"},
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
                "permissions": {"everyone": "viewer"},
            },
            "computation_polling_interval": 180,
            "data_asset_ready_polling_interval": 10,
        }
        self.assertEqual(
            expected_model_json,
            json.loads(settings.model_dump_json(exclude_none=True)),
        )


if __name__ == "__main__":
    unittest.main()
