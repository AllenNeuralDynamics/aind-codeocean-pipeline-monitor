"""Test methods in job module"""

import json
import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from codeocean import CodeOcean
from codeocean.components import EveryoneRole, Permissions
from codeocean.computation import (
    Computation,
    ComputationState,
    DataAssetsRunParam,
    DownloadFileURL,
    Folder,
    FolderItem,
    RunParams,
)
from codeocean.data_asset import (
    AWSS3Target,
    ComputationSource,
    DataAsset,
    DataAssetParams,
    DataAssetState,
    DataAssetType,
    Source,
    Target,
)
from requests import Response
from requests.exceptions import HTTPError
from tenacity import RetryError

from aind_codeocean_pipeline_monitor.job import PipelineMonitorJob
from aind_codeocean_pipeline_monitor.models import (
    CaptureSettings,
    PipelineMonitorSettings,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"


class TestPipelineMonitorJob(unittest.TestCase):
    """Test PipelineMonitorJob class"""

    @classmethod
    def setUpClass(cls) -> None:
        """Set default example settings"""

        with open(RESOURCES_DIR / "expected_log_output.txt", "r") as f:
            expected_run_logs = f.read().splitlines()

        with open(RESOURCES_DIR / "data_description.json", "r") as f:
            expected_data_description = json.dumps(json.load(f))

        no_capture_result_settings = PipelineMonitorSettings(
            run_params=RunParams(
                pipeline_id="abc-123",
            )
        )
        capture_results_settings = PipelineMonitorSettings(
            run_params=RunParams(
                pipeline_id="abc-123",
                data_assets=[
                    DataAssetsRunParam(
                        id="abc-001",
                        mount="ecephys",
                    )
                ],
            ),
            capture_settings=CaptureSettings(
                tags=["derived"],
            ),
        )
        too_many_requests_response = Response()
        too_many_requests_response.status_code = 429
        too_many_request_error = HTTPError()
        too_many_request_error.response = too_many_requests_response

        internal_server_error_response = Response()
        internal_server_error_response.status_code = 500
        internal_server_error = HTTPError()
        internal_server_error.response = internal_server_error_response

        co_client = CodeOcean(domain="test_domain", token="token")
        no_capture_job = PipelineMonitorJob(
            job_settings=no_capture_result_settings, client=co_client
        )
        capture_job = PipelineMonitorJob(
            job_settings=capture_results_settings, client=co_client
        )
        cls.no_capture_job = no_capture_job
        cls.capture_job = capture_job
        cls.too_many_requests_error = too_many_request_error
        cls.internal_server_error = internal_server_error
        cls.expected_run_logs = expected_run_logs
        cls.expected_data_description = expected_data_description

    @patch("codeocean.computation.Computations.get_computation")
    @patch("tenacity.nap.time.sleep", return_value=None)
    @patch("codeocean.computation.sleep", return_value=None)
    def test_monitor_pipeline(
        self,
        mock_sleep: MagicMock,
        mock_nap_sleep: MagicMock,
        mock_get_computation: MagicMock,
    ):
        """Tests _monitor_pipeline method with successful completion"""
        completed_comp = Computation(
            id="c123",
            created=0,
            name="c_name",
            state=ComputationState.Completed,
            run_time=100,
        )
        mock_get_computation.side_effect = [
            Computation(
                id="c123",
                created=0,
                name="c_name",
                state=ComputationState.Running,
                run_time=1,
            ),
            self.too_many_requests_error,
            completed_comp,
        ]
        response = self.no_capture_job._monitor_pipeline(
            computation=Computation(
                id="c123",
                created=0,
                name="c_name",
                state=ComputationState.Initializing,
                run_time=0,
            )
        )
        self.assertEqual(completed_comp, response)
        mock_sleep.assert_called_once_with(180)
        mock_nap_sleep.assert_called_once()

    @patch("codeocean.computation.Computations.get_computation")
    @patch("tenacity.nap.time.sleep", return_value=None)
    @patch("codeocean.computation.sleep", return_value=None)
    def test_monitor_pipeline_error(
        self,
        mock_sleep: MagicMock,
        mock_nap_sleep: MagicMock,
        mock_get_computation: MagicMock,
    ):
        """Tests _monitor_pipeline method with internal server error"""
        mock_get_computation.side_effect = self.internal_server_error
        with self.assertRaises(HTTPError):
            self.no_capture_job._monitor_pipeline(
                computation=Computation(
                    id="c123",
                    created=0,
                    name="c_name",
                    state=ComputationState.Initializing,
                    run_time=0,
                )
            )
        mock_sleep.assert_not_called()
        mock_nap_sleep.assert_not_called()

    @patch("codeocean.computation.Computations.get_computation")
    @patch("tenacity.nap.time.sleep", return_value=None)
    @patch("codeocean.computation.sleep", return_value=None)
    def test_monitor_pipeline_too_many_retries(
        self,
        mock_sleep: MagicMock,
        mock_nap_sleep: MagicMock,
        mock_get_computation: MagicMock,
    ):
        """Tests _monitor_pipeline method with too many retries"""
        mock_get_computation.side_effect = self.too_many_requests_error
        with self.assertRaises(RetryError):
            self.no_capture_job._monitor_pipeline(
                computation=Computation(
                    id="c123",
                    created=0,
                    name="c_name",
                    state=ComputationState.Initializing,
                    run_time=0,
                )
            )

        mock_sleep.assert_not_called()
        self.assertEqual(6, len(mock_nap_sleep.mock_calls))

    @patch("codeocean.computation.Computations.get_computation")
    @patch("tenacity.nap.time.sleep", return_value=None)
    @patch("codeocean.computation.sleep", return_value=None)
    def test_monitor_pipeline_failed(
        self,
        mock_sleep: MagicMock,
        mock_nap_sleep: MagicMock,
        mock_get_computation: MagicMock,
    ):
        """Tests _monitor_pipeline method with failed completion"""
        failed_comp = Computation(
            id="c123",
            created=0,
            name="c_name",
            state=ComputationState.Failed,
            run_time=100,
        )
        mock_get_computation.side_effect = [
            Computation(
                id="c123",
                created=0,
                name="c_name",
                state=ComputationState.Running,
                run_time=1,
            ),
            failed_comp,
        ]
        with self.assertRaises(Exception) as e:
            self.no_capture_job._monitor_pipeline(
                computation=Computation(
                    id="c123",
                    created=0,
                    name="c_name",
                    state=ComputationState.Initializing,
                    run_time=0,
                )
            )
        self.assertIn("The pipeline run failed", e.exception.args[0])
        mock_sleep.assert_called_once_with(180)
        mock_nap_sleep.assert_not_called()

    @patch("codeocean.data_asset.DataAssets.get_data_asset")
    @patch("tenacity.nap.time.sleep", return_value=None)
    @patch("codeocean.data_asset.sleep", return_value=None)
    def test_wait_for_data_asset(
        self,
        mock_sleep: MagicMock,
        mock_nap_sleep: MagicMock,
        mock_get_data_asset: MagicMock,
    ):
        """Tests wait for Data asset success."""
        initial_data_asset = DataAsset(
            id="def-123",
            created=1,
            name="da_name",
            mount="da_mount",
            state=DataAssetState.Draft,
            type=DataAssetType.Result,
            last_used=1,
        )
        completed_data_asset = DataAsset(
            id="def-123",
            created=1,
            name="da_name",
            mount="da_mount",
            state=DataAssetState.Ready,
            type=DataAssetType.Result,
            last_used=1,
        )
        mock_get_data_asset.side_effect = [
            initial_data_asset,
            self.too_many_requests_error,
            completed_data_asset,
        ]

        response = self.capture_job._wait_for_data_asset(initial_data_asset)

        self.assertEqual(completed_data_asset, response)
        mock_sleep.assert_called_once_with(10)
        mock_nap_sleep.assert_called_once()

    @patch("codeocean.data_asset.DataAssets.get_data_asset")
    @patch("tenacity.nap.time.sleep", return_value=None)
    @patch("codeocean.data_asset.sleep", return_value=None)
    def test_wait_for_data_asset_error(
        self,
        mock_sleep: MagicMock,
        mock_nap_sleep: MagicMock,
        mock_get_data_asset: MagicMock,
    ):
        """Tests _wait_for_data_asset method with internal server error"""
        initial_data_asset = DataAsset(
            id="def-123",
            created=1,
            name="da_name",
            mount="da_mount",
            state=DataAssetState.Draft,
            type=DataAssetType.Result,
            last_used=1,
        )
        mock_get_data_asset.side_effect = self.internal_server_error
        with self.assertRaises(HTTPError):
            self.capture_job._wait_for_data_asset(initial_data_asset)
        mock_sleep.assert_not_called()
        mock_nap_sleep.assert_not_called()

    @patch("codeocean.data_asset.DataAssets.get_data_asset")
    @patch("tenacity.nap.time.sleep", return_value=None)
    @patch("codeocean.data_asset.sleep", return_value=None)
    def test_wait_for_data_asset_too_many_retries(
        self,
        mock_sleep: MagicMock,
        mock_nap_sleep: MagicMock,
        mock_get_data_asset: MagicMock,
    ):
        """Tests _monitor_pipeline method with too many retries"""
        mock_get_data_asset.side_effect = self.too_many_requests_error
        initial_data_asset = DataAsset(
            id="def-123",
            created=1,
            name="da_name",
            mount="da_mount",
            state=DataAssetState.Draft,
            type=DataAssetType.Result,
            last_used=1,
        )
        with self.assertRaises(RetryError):
            self.capture_job._wait_for_data_asset(initial_data_asset)
        mock_sleep.assert_not_called()
        self.assertEqual(6, len(mock_nap_sleep.mock_calls))

    @patch("codeocean.data_asset.DataAssets.get_data_asset")
    @patch("tenacity.nap.time.sleep", return_value=None)
    @patch("codeocean.data_asset.sleep", return_value=None)
    def test_wait_for_data_asset_failed(
        self,
        mock_sleep: MagicMock,
        mock_nap_sleep: MagicMock,
        mock_get_data_asset: MagicMock,
    ):
        """Tests _monitor_pipeline method with failed completion"""
        initial_data_asset = DataAsset(
            id="def-123",
            created=1,
            name="da_name",
            mount="da_mount",
            state=DataAssetState.Draft,
            type=DataAssetType.Result,
            last_used=1,
        )
        completed_data_asset = DataAsset(
            id="def-123",
            created=1,
            name="da_name",
            mount="da_mount",
            state=DataAssetState.Failed,
            type=DataAssetType.Result,
            last_used=1,
        )
        mock_get_data_asset.side_effect = [
            initial_data_asset,
            completed_data_asset,
        ]
        with self.assertRaises(Exception) as e:
            self.capture_job._wait_for_data_asset(initial_data_asset)
        self.assertIn("Data asset creation failed", e.exception.args[0])
        mock_sleep.assert_called_once_with(10)
        mock_nap_sleep.assert_not_called()

    @patch("codeocean.data_asset.DataAssets.get_data_asset")
    def test_get_input_data_name(self, mock_get_data_asset: MagicMock):
        """Tests _get_input_data_name success"""
        mock_get_data_asset.return_value = DataAsset(
            id="abc-001",
            created=0,
            name="ecephys_123456_2020-10-10_00-00-00",
            mount="ecephys",
            state=DataAssetState.Ready,
            type=DataAssetType.Dataset,
            last_used=1,
        )
        input_data_name = self.capture_job._get_input_data_name()
        self.assertEqual("ecephys_123456_2020-10-10_00-00-00", input_data_name)

    @patch("codeocean.data_asset.DataAssets.get_data_asset")
    def test_get_input_data_name_none(self, mock_get_data_asset: MagicMock):
        """Tests _get_input_data_name when no input data attached"""
        input_data_name = self.no_capture_job._get_input_data_name()
        self.assertIsNone(input_data_name)
        mock_get_data_asset.assert_not_called()

    @patch("codeocean.computation.Computations.list_computation_results")
    @patch("codeocean.computation.Computations.get_result_file_download_url")
    @patch("aind_codeocean_pipeline_monitor.job.urlopen")
    def test_get_name_from_data_description(
        self,
        mock_url_open: MagicMock,
        mock_get_result_file_url: MagicMock,
        mock_list_comp_results: MagicMock,
    ):
        """Tests _get_name_from_data_description"""
        mock_list_comp_results.return_value = Folder(
            items=[
                FolderItem(name="output", path="output", type=""),
                FolderItem(
                    name="data_description.json",
                    path="data_description.json",
                    type="",
                ),
            ]
        )
        mock_get_result_file_url.return_value = DownloadFileURL(
            url="some_download_url"
        )
        mock_read = MagicMock()
        mock_read.read.return_value = self.expected_data_description.encode(
            "utf-8"
        )
        mock_url_open.return_value.__enter__.return_value = mock_read
        name = self.capture_job._get_name_from_data_description(
            computation=Computation(
                id="c123",
                created=0,
                name="c_name",
                state=ComputationState.Completed,
                run_time=100,
            )
        )
        expected_name_from_file = (
            "ecephys_709351_2024-04-10_14-53-09_sorted_2024-04-19_23-19-34"
        )
        self.assertEqual(expected_name_from_file, name)

    @patch("codeocean.computation.Computations.list_computation_results")
    @patch("codeocean.computation.Computations.get_result_file_download_url")
    @patch("aind_codeocean_pipeline_monitor.job.urlopen")
    def test_get_name_from_data_description_none(
        self,
        mock_url_open: MagicMock,
        mock_get_result_file_url: MagicMock,
        mock_list_comp_results: MagicMock,
    ):
        """Tests _get_name_from_data_description when no file is found."""
        mock_list_comp_results.return_value = Folder(
            items=[
                FolderItem(name="output", path="output", type=""),
            ]
        )
        name = self.capture_job._get_name_from_data_description(
            computation=Computation(
                id="c123",
                created=0,
                name="c_name",
                state=ComputationState.Completed,
                run_time=100,
            )
        )
        self.assertIsNone(name)
        mock_url_open.assert_not_called()
        mock_get_result_file_url.assert_not_called()

    @patch("aind_codeocean_pipeline_monitor.job.datetime")
    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob"
        "._get_input_data_name"
    )
    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob"
        "._get_name_from_data_description"
    )
    def test_get_name(
        self,
        mock_get_name_from_data_description: MagicMock,
        mock_get_input_data_name: MagicMock,
        mock_dt: MagicMock,
    ):
        """Tests _get_name from settings"""

        mock_get_name_from_data_description.return_value = None
        mock_get_input_data_name.return_value = (
            "ecephys_123456_2020-10-10_00-00-00"
        )
        mock_dt.now.return_value = datetime(2020, 11, 10)
        completed_comp = Computation(
            id="c123",
            created=0,
            name="c_name",
            state=ComputationState.Completed,
            run_time=100,
        )
        name = self.capture_job._get_name(computation=completed_comp)
        expected_name = (
            "ecephys_123456_2020-10-10_00-00-00_processed_2020-11-10_00-00-00"
        )
        self.assertEqual(expected_name, name)

    @patch("aind_codeocean_pipeline_monitor.job.datetime")
    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob"
        "._get_input_data_name"
    )
    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob"
        "._get_name_from_data_description"
    )
    def test_get_name_from_dd(
        self,
        mock_get_name_from_data_description: MagicMock,
        mock_get_input_data_name: MagicMock,
        mock_dt: MagicMock,
    ):
        """Tests _get_name from data_description file"""

        mock_get_name_from_data_description.return_value = (
            "ecephys_123456_2020-10-10_00-00-00_sorted_2020-11-10_00-00-00"
        )
        mock_get_input_data_name.return_value = (
            "ecephys_123456_2020-10-10_00-00-00"
        )
        mock_dt.now.return_value = datetime(2020, 11, 10)
        completed_comp = Computation(
            id="c123",
            created=0,
            name="c_name",
            state=ComputationState.Completed,
            run_time=100,
        )
        name = self.capture_job._get_name(computation=completed_comp)
        self.assertEqual(
            "ecephys_123456_2020-10-10_00-00-00_sorted_2020-11-10_00-00-00",
            name,
        )

    @patch("aind_codeocean_pipeline_monitor.job.datetime")
    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob"
        "._get_input_data_name"
    )
    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob"
        "._get_name_from_data_description"
    )
    def test_get_name_from_dd_bad_format(
        self,
        mock_get_name_from_data_description: MagicMock,
        mock_get_input_data_name: MagicMock,
        mock_dt: MagicMock,
    ):
        """Tests _get_name from data_description file when name in file is
        not in the correct format."""

        mock_get_name_from_data_description.return_value = (
            "ecephys_123456_2020-10-1_sorted_2020-11-10_00-00-00"
        )
        mock_get_input_data_name.return_value = (
            "ecephys_123456_2020-10-10_00-00-00"
        )
        mock_dt.now.return_value = datetime(2020, 11, 10)
        completed_comp = Computation(
            id="c123",
            created=0,
            name="c_name",
            state=ComputationState.Completed,
            run_time=100,
        )
        with self.assertLogs() as captured:
            name = self.capture_job._get_name(computation=completed_comp)
        expected_logs = [
            "WARNING:root:Name in data description "
            "ecephys_123456_2020-10-1_sorted_2020-11-10_00-00-00 "
            "does not match expected pattern! "
            "Will attempt to set default."
        ]
        self.assertEqual(expected_logs, captured.output)
        self.assertEqual(
            "ecephys_123456_2020-10-10_00-00-00_processed_2020-11-10_00-00-00",
            name,
        )

    @patch("aind_codeocean_pipeline_monitor.job.datetime")
    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob"
        "._get_input_data_name"
    )
    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob"
        "._get_name_from_data_description"
    )
    def test_get_name_error(
        self,
        mock_get_name_from_data_description: MagicMock,
        mock_get_input_data_name: MagicMock,
        mock_dt: MagicMock,
    ):
        """Tests _get_name when input data name is None and data_description
        name is None"""

        mock_get_name_from_data_description.return_value = None
        mock_get_input_data_name.return_value = None
        mock_dt.now.return_value = datetime(2020, 11, 10)
        completed_comp = Computation(
            id="c123",
            created=0,
            name="c_name",
            state=ComputationState.Completed,
            run_time=100,
        )
        with self.assertRaises(Exception) as e:
            self.capture_job._get_name(computation=completed_comp)

        self.assertEqual(
            "Unable to construct data asset name.", e.exception.args[0]
        )

    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob" "._get_name"
    )
    @patch("aind_codeocean_pipeline_monitor.job.datetime")
    def test_build_data_asset_params(
        self, mock_dt: MagicMock, mock_get_name: MagicMock
    ):
        """Tests _build_data_asset_params method"""

        mock_dt.now.return_value = datetime(2020, 11, 10)
        completed_comp = Computation(
            id="c123",
            created=0,
            name="c_name",
            state=ComputationState.Completed,
            run_time=100,
        )
        mock_get_name.return_value = (
            "ecephys_123456_2020-10-10_00-00-00_processed_2020-11-10_00-00-00"
        )
        params = self.capture_job._build_data_asset_params(
            monitor_pipeline_response=completed_comp
        )
        expected_name = (
            "ecephys_123456_2020-10-10_00-00-00_processed_2020-11-10_00-00-00"
        )
        expected_params = DataAssetParams(
            name=expected_name,
            tags=["derived"],
            mount=expected_name,
            source=Source(
                computation=ComputationSource(id="c123", path=None),
            ),
        )

        self.assertEqual(expected_params, params)

    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob" "._get_name"
    )
    @patch("aind_codeocean_pipeline_monitor.job.datetime")
    def test_build_data_asset_params_with_name_mount(
        self, mock_dt: MagicMock, mock_get_name: MagicMock
    ):
        """Tests _build_data_asset_params method when name and mount are set"""

        mock_get_name.return_value = (
            "ecephys_123456_2020-10-10_00-00-00_processed_2020-11-10_00-00-00"
        )

        mock_dt.now.return_value = datetime(2020, 11, 10)
        completed_comp = Computation(
            id="c123",
            created=0,
            name="c_name",
            state=ComputationState.Completed,
            run_time=100,
        )
        settings2 = self.capture_job.job_settings.model_copy(deep=True)
        settings2.capture_settings.name = "foo"
        settings2.capture_settings.mount = "bar"
        job = PipelineMonitorJob(
            job_settings=settings2, client=CodeOcean(domain="", token="")
        )
        expected_params = DataAssetParams(
            name="foo",
            tags=["derived"],
            mount="bar",
            source=Source(
                computation=ComputationSource(id="c123", path=None),
            ),
        )
        params = job._build_data_asset_params(
            monitor_pipeline_response=completed_comp
        )

        self.assertEqual(expected_params, params)

    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob" "._get_name"
    )
    @patch("aind_codeocean_pipeline_monitor.job.datetime")
    def test_build_data_asset_params_with_target(
        self, mock_dt: MagicMock, mock_get_name: MagicMock
    ):
        """Tests _build_data_asset_params method when target is set"""

        mock_dt.now.return_value = datetime(2020, 11, 10)
        mock_get_name.return_value = (
            "ecephys_123456_2020-10-10_00-00-00_processed_2020-11-10_00-00-00"
        )
        completed_comp = Computation(
            id="c123",
            created=0,
            name="c_name",
            state=ComputationState.Completed,
            run_time=100,
        )
        settings2 = self.capture_job.job_settings.model_copy(deep=True)
        settings2.capture_settings.target = Target(
            aws=AWSS3Target(bucket="ext_bucket", prefix="")
        )
        job = PipelineMonitorJob(
            job_settings=settings2, client=CodeOcean(domain="", token="")
        )
        expected_name = (
            "ecephys_123456_2020-10-10_00-00-00_processed_2020-11-10_00-00-00"
        )
        expected_params = DataAssetParams(
            name=expected_name,
            tags=["derived"],
            mount=expected_name,
            source=Source(
                computation=ComputationSource(id="c123", path=None),
            ),
            target=Target(
                aws=AWSS3Target(
                    bucket="ext_bucket",
                    prefix=expected_name,
                )
            ),
        )
        params = job._build_data_asset_params(
            monitor_pipeline_response=completed_comp
        )

        self.assertEqual(expected_params, params)

    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob"
        "._build_data_asset_params"
    )
    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob"
        "._wait_for_data_asset"
    )
    @patch(
        "aind_codeocean_pipeline_monitor.job.PipelineMonitorJob"
        "._monitor_pipeline"
    )
    @patch("codeocean.data_asset.DataAssets.create_data_asset")
    @patch("codeocean.computation.Computations.run_capsule")
    @patch("codeocean.data_asset.DataAssets.update_permissions")
    @patch("aind_codeocean_pipeline_monitor.job.datetime")
    def test_run_job(
        self,
        mock_datetime: MagicMock,
        mock_update_permissions: MagicMock,
        mock_run_capsule: MagicMock,
        mock_create_data_asset: MagicMock,
        mock_monitor_pipeline: MagicMock,
        mock_wait_for_data_asset: MagicMock,
        mock_build_data_asset_parms: MagicMock,
    ):
        """Tests steps are called in run_job method"""
        mock_datetime.now.return_value = datetime(2020, 11, 10)
        mock_run_capsule.return_value = Computation(
            id="c123",
            created=0,
            name="c_name",
            state=ComputationState.Initializing,
            run_time=0,
        )
        mock_monitor_pipeline.return_value = Computation(
            id="c123",
            created=0,
            name="c_name",
            state=ComputationState.Completed,
            run_time=100,
        )

        expected_name = (
            "ecephys_123456_2020-10-10_00-00-00_processed_2020-11-10_00-00-00"
        )
        mock_create_data_asset.return_value = DataAsset(
            id="def-123",
            created=1,
            name=expected_name,
            mount=expected_name,
            state=DataAssetState.Draft,
            type=DataAssetType.Result,
            last_used=1,
            tags=["derived"],
        )
        mock_wait_for_data_asset.return_value = DataAsset(
            id="def-123",
            created=1,
            name=expected_name,
            mount=expected_name,
            state=DataAssetState.Ready,
            type=DataAssetType.Result,
            last_used=1,
            tags=["derived"],
        )

        mock_build_data_asset_parms.return_value = DataAssetParams(
            name=expected_name,
            tags=["derived"],
            mount=expected_name,
            source=Source(
                computation=ComputationSource(id="c123", path=None),
            ),
        )
        with self.assertLogs(level="INFO") as captured:
            self.capture_job.run_job()

        self.assertEqual(self.expected_run_logs, captured.output)
        mock_update_permissions.assert_called_once_with(
            data_asset_id="def-123",
            permissions=Permissions(everyone=EveryoneRole.Viewer),
        )


if __name__ == "__main__":
    unittest.main()
