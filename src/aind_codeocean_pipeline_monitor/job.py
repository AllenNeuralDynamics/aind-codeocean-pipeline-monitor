"""Main module to run and monitor a pipeline."""

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.request import urlopen
from zoneinfo import ZoneInfo

import requests
from aind_data_access_api.document_db import MetadataDbClient
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

try:
    from aind_alert_utils.teams import create_body_contents
    from aind_data_schema_models.data_name_patterns import DataLevel, DataRegex
except ModuleNotFoundError:  # pragma: no cover
    raise ModuleNotFoundError(
        "Running jobs requires all dependencies: "
        "'pip install aind-codeocean-pipeline-monitor[full]'. "
        "See README for more information."
    )
import os

from codeocean import CodeOcean
from codeocean.computation import (
    Computation,
    ComputationEndStatus,
    ComputationState,
)
from codeocean.data_asset import (
    AWSS3Target,
    ComputationSource,
    DataAsset,
    DataAssetOrigin,
    DataAssetParams,
    DataAssetState,
    Source,
    Target,
)
from codeocean.error import Error

from aind_codeocean_pipeline_monitor.models import PipelineMonitorSettings

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


class PipelineMonitorJob:
    """Class to run a PipelineMonitor Job"""

    def __init__(
        self, job_settings: PipelineMonitorSettings, client: CodeOcean
    ):
        """Class constructor"""
        self.job_settings = job_settings
        self.client = client

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
                    timeout=self.job_settings.computation_timeout,
                )
            )
            if (
                wait_until_completed_response.state == ComputationState.Failed
                or wait_until_completed_response.end_status
                in [ComputationEndStatus.Failed, ComputationEndStatus.Stopped]
                or not (
                    wait_until_completed_response.exit_code is None
                    or wait_until_completed_response.exit_code == 0
                )
            ):
                raise Exception(
                    f"The computation had an error: "
                    f"{wait_until_completed_response}"
                )
            return wait_until_completed_response
        except TimeoutError as e:
            logging.error(
                f"Computation timeout reached: {e.args}, attempting to "
                f"terminate pipeline"
            )
            self.client.computations.delete_computation(
                computation_id=computation.id
            )
            raise e

    def _wait_for_data_asset(self, create_data_asset_response) -> DataAsset:
        """
        Wait for data asset to be available. Will retry if TooManyRequests.
        Parameters
        ----------
        create_data_asset_response : DataAsset

        Returns
        -------

        """
        wait_until_ready_response = self.client.data_assets.wait_until_ready(
            data_asset=create_data_asset_response,
            polling_interval=(
                self.job_settings.data_asset_ready_polling_interval
            ),
            timeout=self.job_settings.data_asset_ready_timeout,
        )
        if wait_until_ready_response.state == DataAssetState.Failed:
            raise Exception(
                f"Data asset creation failed: {wait_until_ready_response}"
            )
        return wait_until_ready_response

    def _send_alert_to_teams(
        self, message: str, extra_text: Optional[str] = None
    ):
        """
        Send an alert to MS Teams.

        Parameters
        ----------
        message : str
        extra_text : Optional[str]

        """

        post_request_contents = create_body_contents(
            message=message, extra_text=extra_text
        )
        response = requests.post(
            url=self.job_settings.alert_url, json=post_request_contents
        )
        if response.status_code == 200 or response.status_code == 202:
            logging.info(f"Alert response: {response.text}")
        else:
            logging.warning(
                f"There was an issue sending the alert: {response}"
            )

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

    @staticmethod
    def _get_name_and_level_from_data_description(
        data_description: Optional[Dict[str, Any]],
    ) -> Dict[str, Optional[str]]:
        """
        Attempts to extract a name and data level from the data_description
        if the data_description is found.

        Parameters
        ----------
        data_description : Optional[Dict[str, Any]]

        Returns
        -------
        Dict[str, Optional[str]]
          {'name': Optional[str], 'data_level': Optional[str]}

        """

        if data_description is not None:
            return {
                "name": data_description.get("name"),
                "data_level": data_description.get("data_level"),
            }
        else:
            return {"name": None, "data_level": None}

    def _get_name(
        self,
        input_data_name: Optional[str],
        data_description: Optional[Dict[str, Any]],
    ) -> str:
        """
        Get a data asset name. Will try to use the name from a
        data_description.json file. If file does not exist, then will build a
        default name using the input_data_name, process_name_suffix, and
        process_name_suffix_tz fields defined in CapturedDataAssetParams.

        Parameters
        ----------
        input_data_name : Optional[str]
          Name of the input data asset. The computation only stores the id.
        data_description : Optional[Dict[str, Any]]
          The data_description.json contents if available.

        Returns
        -------
        str

        """

        capture_params = self.job_settings.capture_settings
        dt = datetime.now(
            tz=ZoneInfo(
                self.job_settings.capture_settings.process_name_suffix_tz
            )
        )
        suffix = capture_params.process_name_suffix
        dt_suffix = dt.strftime("%Y-%m-%d_%H-%M-%S")

        default_name = f"{input_data_name}_{suffix}_{dt_suffix}"

        info_from_file = self._get_name_and_level_from_data_description(
            data_description
        )
        name_from_file = info_from_file.get("name")
        level_from_file = info_from_file.get("data_level")
        if level_from_file != DataLevel.DERIVED.value:
            logging.warning(
                f"Data level in data description {level_from_file} "
                f"does not match expected pattern! Ignoring name in data "
                f"description and will attempt to set a default name."
            )
            name_from_file = None
        elif (
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
        self,
        monitor_pipeline_response: Computation,
        input_data_name: Optional[str],
        data_description: Optional[Dict[str, Any]],
    ) -> DataAssetParams:
        """
        Build DataAssetParams model from CapturedDataAssetParams and
        Computation from monitor_pipeline_response

        Parameters
        ----------
        monitor_pipeline_response : Computation
          The Computation from monitor_pipeline_response. If Target is set to
          AWSS3Target, prefix will be overridden with data asset name.
        input_data_name : Optional[str]
          Name of the input data asset that was converted to a result
        data_description: Dict[str, Dict[str, Any]]
          The data_description.json contents if available.

        Returns
        -------
        DataAssetParams

        """
        if self.job_settings.capture_settings.name is not None:
            data_asset_name = self.job_settings.capture_settings.name
        else:
            data_asset_name = self._get_name(
                data_description=data_description,
                input_data_name=input_data_name,
            )
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

    def _gather_metadata(
        self, computation: Computation, core_metadata_name: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Download a metadata file from the top level results folder into
        a single dictionary
        Parameters
        ----------
        computation : Computation
        core_metadata_name : str
          Name of core metadata file to download, without .json suffix

        Returns
        -------
        Dict[str, Dict[str, Any]]

        """
        result_files = self.client.computations.list_computation_results(
            computation_id=computation.id
        )
        core_json = dict()
        core_metadata_json_name = f"{core_metadata_name}.json"
        if core_metadata_json_name in [r.path for r in result_files.items]:
            download_url = (
                self.client.computations.get_result_file_download_url(
                    computation_id=computation.id,
                    path=core_metadata_json_name,
                )
            )
            with urlopen(download_url.url) as f:
                contents = f.read().decode("utf-8")
            core_json = json.loads(contents)
        return core_json

    def _update_docdb(
        self,
        wait_for_data_asset_response: DataAsset,
        computation: Computation,
        name: str,
    ) -> None:
        """
        Add a record in DocDB for the newly created Result
        using the docdb registration api.

        Parameters
        ----------
        wait_for_data_asset_response : DataAsset
        name : str
          The name of the data asset.
        computation : Computation
          The computation that created the data asset.

        Returns
        -------
        None
          Modifies the DocDB Database

        """

        docdb_settings = self.job_settings.capture_settings.docdb_settings
        capture_result_source = wait_for_data_asset_response.source_bucket
        codeocean_id = wait_for_data_asset_response.id
        computation_id = computation.id
        # We only index data assets in AWS for now
        if capture_result_source is None or (
            capture_result_source is not None
            and capture_result_source.external is not True
            and docdb_settings.results_bucket is not None
        ):
            bucket = docdb_settings.results_bucket
            prefix = wait_for_data_asset_response.id
        elif (
            capture_result_source is not None
            and capture_result_source.external is True
            and capture_result_source.origin == DataAssetOrigin.AWS
            and capture_result_source.bucket is not None
            and capture_result_source.prefix is not None
        ):
            bucket = capture_result_source.bucket
            prefix = capture_result_source.prefix
        else:
            raise ValueError(
                f"Unable to add record to DocDB using "
                f"{wait_for_data_asset_response} and {docdb_settings}!"
            )
        location = f"s3://{bucket}/{prefix}"
        if docdb_settings is not None:
            retry = Retry(
                total=3,
                backoff_factor=20,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "POST"],
            )
            adapter = HTTPAdapter(max_retries=retry)
            session = requests.Session()
            session.mount("https://", adapter)
            with MetadataDbClient(
                host=docdb_settings.docdb_api_gateway,
                version=docdb_settings.docdb_version,
                session=session,
            ) as docdb_client:
                register_response = docdb_client.register_co_result(
                    s3_location=location,
                    name=name,
                    co_asset_id=codeocean_id,
                    co_computation_id=computation_id,
                )
                logging.info(f"DocDB register_co_result: {register_response}")

    def run_job(self):
        """
        Run pipeline monitor job. If captured_data_asset_params is not
        None, then will capture result.
        """

        input_data_name = self._get_input_data_name()
        try:
            logging.info(
                f"Starting job with: {self.job_settings}. "
                f"Input data: {input_data_name}"
            )
            if self.job_settings.alert_url is not None:
                message = f"Starting {input_data_name}"
                params = [
                    ("capsule", self.job_settings.run_params.capsule_id),
                    ("pipeline", self.job_settings.run_params.pipeline_id),
                    ("version", self.job_settings.run_params.version),
                ]
                extra_text = "".join(
                    f"- {label}: {value}\n"
                    for label, value in params
                    if value is not None
                )
                self._send_alert_to_teams(
                    message=message, extra_text=extra_text
                )

            start_pipeline_response = self.client.computations.run_capsule(
                self.job_settings.run_params
            )
            logging.info(f"start_pipeline_response: {start_pipeline_response}")
            monitor_pipeline_response = self._monitor_pipeline(
                start_pipeline_response
            )
            logging.info(
                f"monitor_pipeline_response: {monitor_pipeline_response}"
            )
            if self.job_settings.capture_settings is not None:
                logging.info("Capturing result")
                data_description = self._gather_metadata(
                    computation=monitor_pipeline_response,
                    core_metadata_name="data_description",
                )
                data_asset_params = self._build_data_asset_params(
                    monitor_pipeline_response=monitor_pipeline_response,
                    input_data_name=input_data_name,
                    data_description=data_description,
                )
                capture_result_response = (
                    self.client.data_assets.create_data_asset(
                        data_asset_params=data_asset_params
                    )
                )
                logging.info(
                    f"capture_result_response: {capture_result_response}"
                )
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
                if (
                    self.job_settings.capture_settings.docdb_settings
                    is not None
                ):
                    logging.info(
                        f"Updating DocDB: for {data_asset_params.name} "
                    )
                    try:
                        self._update_docdb(
                            name=data_asset_params.name,
                            wait_for_data_asset_response=wait_for_data_asset,
                            computation=monitor_pipeline_response,
                        )
                    except Exception as e:
                        logging.error(
                            f"Error updating DocDB: {e.args}. "
                            f"Continuing with job."
                        )

            logging.info("Finished job.")
            if self.job_settings.alert_url is not None:
                message = f"Finished {input_data_name}"
                self._send_alert_to_teams(message=message)
        except (Error, Exception) as e:
            if self.job_settings.alert_url is not None:
                message = f"Error with {input_data_name}"
                extra_text = (
                    str(e) if isinstance(e, Error) else f"Message: {e.args}"
                )
                self._send_alert_to_teams(
                    message=message, extra_text=extra_text
                )
            raise e
