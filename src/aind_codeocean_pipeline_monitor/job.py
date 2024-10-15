"""Main module to run and monitor a pipeline."""

import json
import logging
import re
from datetime import datetime
from typing import Optional
from urllib.request import urlopen
from zoneinfo import ZoneInfo

from aind_data_schema_models.data_name_patterns import DataRegex
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
                self.client.computations.wait_until_completed(
                    computation=computation,
                    polling_interval=(
                        self.job_settings.computation_polling_interval
                    ),
                )
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
                    data_asset=create_data_asset_response,
                    polling_interval=(
                        self.job_settings.data_asset_ready_polling_interval
                    ),
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

    def _get_input_data_name(self) -> Optional[str]:
        """Get the name of the input data asset from the run_params"""

        input_data_assets = self.job_settings.run_params.data_assets
        if input_data_assets:
            first_data_asset = input_data_assets[0]
            input_data_asset = self.client.data_assets.get_data_asset(
                data_asset_id=first_data_asset.id
            )
            return input_data_asset.name
        else:
            return None

    def _get_name_from_data_description(
        self, computation: Computation
    ) -> Optional[str]:
        """
        Attempts to download a data_description file from the 'results' folder
        and then extracts the 'name' from that file.

        Parameters
        ----------
        computation : Computation

        Returns
        -------
        str | None
          The name from that data_description file if found, otherwise None.

        """

        dd_file_name = (
            self.job_settings.capture_settings.data_description_file_name
        )

        result_files = self.client.computations.list_computation_results(
            computation_id=computation.id
        )

        if dd_file_name in [r.path for r in result_files.items]:
            download_url = (
                self.client.computations.get_result_file_download_url(
                    computation_id=computation.id,
                    path=dd_file_name,
                )
            )
            with urlopen(download_url.url) as f:
                contents = f.read().decode("utf-8")
            data_description = json.loads(contents)
            return data_description.get("name")
        else:
            return None

    def _get_name(self, computation: Computation) -> str:
        """
        Get a data asset name. Will try to use the name from a
        data_description.json file. If file does not exist, then will build a
        default name using the input_data_name, process_name_suffix, and
        process_name_suffix_tz fields defined in CapturedDataAssetParams.

        Parameters
        ----------
        computation : Computation
          Uses the computation.id and the code ocean sdk to check if there is
          a data_description.json file in the results folder. Will attempt to
          extract the data_asset_name from that file if found. Otherwise, this
          method will construct a default name using the input data_asset and
          the current datetime in utc.

        Returns
        -------
        str

        """

        capture_params = self.job_settings.capture_settings
        dt = datetime.now(tz=ZoneInfo("UTC"))
        input_data_name = self._get_input_data_name()
        suffix = capture_params.process_name_suffix
        dt_suffix = dt.strftime("%Y-%m-%d_%H-%M-%S")

        default_name = f"{input_data_name}_{suffix}_{dt_suffix}"

        name_from_file = self._get_name_from_data_description(
            computation=computation
        )
        if (
            name_from_file is not None
            and re.match(DataRegex.DERIVED.value, name_from_file) is None
        ):
            logging.warning(
                f"Name in data description {name_from_file} "
                f"does not match expected pattern! "
                f"Will attempt to set default."
            )
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
        if self.job_settings.capture_settings.name is not None:
            data_asset_name = self.job_settings.capture_settings.name
        else:
            data_asset_name = self._get_name(monitor_pipeline_response)
        if self.job_settings.capture_settings.mount is not None:
            data_asset_mount = self.job_settings.capture_settings.mount
        else:
            data_asset_mount = data_asset_name
        if self.job_settings.capture_settings.target is not None:
            prefix = data_asset_name
            bucket = self.job_settings.capture_settings.target.aws.bucket
            target = Target(aws=AWSS3Target(bucket=bucket, prefix=prefix))
        else:
            target = None

        data_asset_params = DataAssetParams(
            name=data_asset_name,
            description=self.job_settings.capture_settings.description,
            mount=data_asset_mount,
            tags=self.job_settings.capture_settings.tags,
            source=Source(
                computation=ComputationSource(
                    id=monitor_pipeline_response.id,
                ),
            ),
            target=target,
            custom_metadata=(
                self.job_settings.capture_settings.custom_metadata
            ),
            results_info=self.job_settings.capture_settings.results_info,
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
        if self.job_settings.capture_settings is not None:
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
                permissions=self.job_settings.capture_settings.permissions,
            )
        logging.info("Finished job.")
