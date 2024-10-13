"""Tests for models module"""

import json
import unittest

from codeocean.computation import RunParams
from codeocean.data_asset import AWSS3Target, Target

from aind_codeocean_pipeline_monitor.models import (
    CapturedDataAssetParams,
    PipelineMonitorSettings,
)


class TestsCapturedDataAssetParams(unittest.TestCase):
    """Tests for CapturedDataAssetParams model"""

    def test_construction(self):
        """Basic model construct."""

        model = CapturedDataAssetParams(
            tags=["derived, 123456, ecephys"],
            description="some data",
            custom_metadata={"data level": "derived"},
        )
        expected_model_json = {
            "tags": ["derived, 123456, ecephys"],
            "description": "some data",
            "permissions": {"everyone": "viewer"},
            "custom_metadata": {"data level": "derived"},
            "process_name_suffix": "processed",
            "process_name_suffix_tz": "UTC",
            "data_description_location": "/results/data_description.json",
        }
        self.assertEqual(
            expected_model_json,
            json.loads(model.model_dump_json(exclude_none=True)),
        )

    def test_set_target(self):
        """Test target can be defined"""
        model = CapturedDataAssetParams(
            tags=["derived, 123456, ecephys"],
            target=Target(aws=AWSS3Target(bucket="my-bucket", prefix="")),
        )
        expected_model_json = {
            "permissions": {"everyone": "viewer"},
            "tags": ["derived, 123456, ecephys"],
            "target": {"aws": {"bucket": "my-bucket", "prefix": ""}},
            "process_name_suffix": "processed",
            "process_name_suffix_tz": "UTC",
            "data_description_location": "/results/data_description.json",
        }
        self.assertTrue(
            expected_model_json, model.model_dump_json(exclude_none=True)
        )


class TestsPipelineMonitorSettings(unittest.TestCase):
    """Tests PipelineMonitorSettings model"""

    def test_basic_construct(self):
        """Test basic model constructor"""
        capture_data_params = CapturedDataAssetParams(
            tags=["derived, 123456, ecephys"],
            custom_metadata={"data level": "derived"},
        )
        run_params = RunParams(pipeline_id="abc-123", version=2)
        settings = PipelineMonitorSettings(
            captured_data_asset_params=capture_data_params,
            run_params=run_params,
        )
        expected_model_json = {
            "run_params": {"pipeline_id": "abc-123", "version": 2},
            "captured_data_asset_params": {
                "tags": ["derived, 123456, ecephys"],
                "custom_metadata": {"data level": "derived"},
                "process_name_suffix": "processed",
                "process_name_suffix_tz": "UTC",
                "data_description_location": "/results/data_description.json",
                "permissions": {"everyone": "viewer"},
            },
        }
        self.assertEqual(
            expected_model_json,
            json.loads(settings.model_dump_json(exclude_none=True)),
        )


if __name__ == "__main__":
    unittest.main()
