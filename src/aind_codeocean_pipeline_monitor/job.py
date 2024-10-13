"""Main module to run and monitor a pipeline."""

import json
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from codeocean import CodeOcean
from codeocean.computation import Computation, ComputationState
from codeocean.data_asset import (
    AWSS3Target,
    ComputationSource,
    DataAsset,
    DataAssetParams,
    DataAssetState,
    Source,
    Target,
)
from requests.exceptions import HTTPError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from aind_codeocean_pipeline_monitor.models import PipelineMonitorSettings


class TooManyRequests(Exception):
    """Too many requests Exception"""


class PipelineMonitorJob:
    """Class to run a PipelineMonitor Job"""

    def __init__(
        self, job_settings: PipelineMonitorSettings, client: CodeOcean
    ):
        """Class constructor"""
        self.job_settings = job_settings
        self.client = client

    @retry(
        retry=retry_if_exception_type(TooManyRequests),
        wait=wait_exponential(multiplier=1, min=8, max=32) + wait_random(0, 5),
        stop=stop_after_attempt(7),
    )
    def _monitor_pipeline(self, computation: Computation) -> Computation:
        """
        Monitor a pipeline. Will retry requests if TooManyRequests.
        Parameters
        ----------
        computation : Computation
          Computation from _start_pipeline response

        Returns
        -------
        Computation

        """
        try:
            wait_until_completed_response = (
                self.client.computations.wait_until_completed(computation)
            )
            if wait_until_completed_response.state == ComputationState.Failed:
                raise Exception(
                    f"The pipeline run failed: {wait_until_completed_response}"
                )
            return wait_until_completed_response
        except HTTPError as err:
            if err.response.status_code == 429:
                raise TooManyRequests
            else:
                raise

    @retry(
        retry=retry_if_exception_type(TooManyRequests),
        wait=wait_exponential(multiplier=1, min=8, max=32) + wait_random(0, 5),
        stop=stop_after_attempt(7),
    )
    def _wait_for_data_asset(self, create_data_asset_response) -> DataAsset:
        """
        Wait for data asset to be available. Will retry if TooManyRequests.
        Parameters
        ----------
        create_data_asset_response : DataAsset

        Returns
        -------

        """
        try:
            wait_until_ready_response = (
                self.client.data_assets.wait_until_ready(
                    create_data_asset_response
                )
            )
            if wait_until_ready_response.state == DataAssetState.Failed:
                raise Exception(
                    f"Data asset creation failed: {wait_until_ready_response}"
                )
            return wait_until_ready_response
        except HTTPError as err:
            if err.response.status_code == 429:
                raise TooManyRequests
            else:
                raise

    def _get_name(self) -> str:
        """
        Get a data asset name. Will try to use the name from a
        data_description.json file. If file does not exist, then will build a
        default name using the input_data_name, process_name_suffix, and
        process_name_suffix_tz fields defined in CapturedDataAssetParams.

        Returns
        -------
        str

        """

        capture_params = self.job_settings.captured_data_asset_params
        dt = datetime.now(tz=ZoneInfo("UTC"))
        input_data_name = capture_params.input_data_name
        suffix = capture_params.process_name_suffix
        dt_suffix = dt.strftime("%Y-%m-%d_%H-%M-%S")

        default_name = f"{input_data_name}_{suffix}_{dt_suffix}"

        data_description_location = capture_params.data_description_location

        if (
            data_description_location is not None
            and Path(capture_params.data_description_location).is_file()
        ):
            with open(data_description_location, "r") as f:
                contents = json.load(f)
            if isinstance(contents, dict):
                name_from_file = contents.get("name")
            else:
                name_from_file = None
        else:
            name_from_file = None
        if name_from_file is None and input_data_name is None:
            raise Exception("Unable to construct data asset name.")
        elif name_from_file is not None:
            return name_from_file
        else:
            return default_name

    def _build_data_asset_params(
        self, monitor_pipeline_response: Computation
    ) -> DataAssetParams:
        """
        Build DataAssetParams model from CapturedDataAssetParams and
        Computation from monitor_pipeline_response

        Parameters
        ----------
        monitor_pipeline_response : Computation
          The Computation from monitor_pipeline_response. If Target is set to
          AWSS3Target, prefix will be overridden with data asset name.

        Returns
        -------
        DataAssetParams

        """
        if self.job_settings.captured_data_asset_params.name is not None:
            data_asset_name = self.job_settings.captured_data_asset_params.name
        else:
            data_asset_name = self._get_name()
        if self.job_settings.captured_data_asset_params.mount is not None:
            data_asset_mount = (
                self.job_settings.captured_data_asset_params.mount
            )
        else:
            data_asset_mount = data_asset_name
        if self.job_settings.captured_data_asset_params.target is not None:
            prefix = data_asset_name
            bucket = (
                self.job_settings.captured_data_asset_params.target.aws.bucket
            )
            target = Target(aws=AWSS3Target(bucket=bucket, prefix=prefix))
        else:
            target = None

        data_asset_params = DataAssetParams(
            name=data_asset_name,
            description=(
                self.job_settings.captured_data_asset_params.description
            ),
            mount=data_asset_mount,
            tags=self.job_settings.captured_data_asset_params.tags,
            source=Source(
                computation=ComputationSource(
                    id=monitor_pipeline_response.id,
                ),
            ),
            target=target,
            custom_metadata=(
                self.job_settings.captured_data_asset_params.custom_metadata
            ),
            results_info=(
                self.job_settings.captured_data_asset_params.results_info
            ),
        )
        return data_asset_params

    def run_job(self):
        """
        Run pipeline monitor job. If captured_data_asset_params is not
        None, then will capture result.
        """

        logging.info(f"Starting job with: {self.job_settings}")
        start_pipeline_response = self.client.computations.run_capsule(
            self.job_settings.run_params
        )
        logging.info(f"start_pipeline_response: {start_pipeline_response}")
        monitor_pipeline_response = self._monitor_pipeline(
            start_pipeline_response
        )
        logging.info(f"monitor_pipeline_response: {monitor_pipeline_response}")
        if self.job_settings.captured_data_asset_params is not None:
            logging.info("Capturing result")
            data_asset_params = self._build_data_asset_params(
                monitor_pipeline_response=monitor_pipeline_response
            )
            capture_result_response = (
                self.client.data_assets.create_data_asset(
                    data_asset_params=data_asset_params
                )
            )
            logging.info(f"capture_result_response: {capture_result_response}")
            wait_for_data_asset = self._wait_for_data_asset(
                create_data_asset_response=capture_result_response
            )
            logging.info(
                f"wait_for_data_asset_response: {wait_for_data_asset}"
            )
            self.client.data_assets.update_permissions(
                data_asset_id=capture_result_response.id,
                permissions=(
                    self.job_settings.captured_data_asset_params.permissions
                ),
            )
        logging.info("Finished job.")
