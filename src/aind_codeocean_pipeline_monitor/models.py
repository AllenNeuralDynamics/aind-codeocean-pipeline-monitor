"""Settings needed to run a Pipeline Monitor Job"""

from typing import Literal, Optional

#  pydantic raises errors if these dataclasses are not imported
from codeocean.components import (  # noqa: F401
    EveryoneRole,
    GroupPermissions,
    GroupRole,
    Permissions,
    UserPermissions,
    UserRole,
)
from codeocean.computation import RunParams
from codeocean.data_asset import (  # noqa: F401
    AWSS3Target,
    DataAssetParams,
    GCPCloudStorageSource,
    ResultsInfo,
    Target,
)
from pydantic import Field
from pydantic_settings import BaseSettings


class CaptureSettings(BaseSettings, DataAssetParams):
    """
    Make name and mount fields optional. They will be determined after the
    pipeline is finished.
    """

    # Override fields from DataAssetParams model
    name: Optional[str] = Field(default=None)
    mount: Optional[str] = Field(default=None)
    # Source of results asset will be determined after pipeline is finished
    source: Literal[None] = Field(default=None)

    # Additional fields
    data_description_file_name: Literal["data_description.json"] = Field(
        default="data_description.json",
        description=(
            "Attempt to create data asset name from this file. We might "
            "import this from the aind-data-schema package directly in future "
            "releases."
        ),
    )
    process_name_suffix: Optional[str] = Field(default="processed")
    process_name_suffix_tz: Optional[str] = Field(default="UTC")
    permissions: Permissions = Field(
        default=Permissions(everyone=EveryoneRole.Viewer),
        description="Permissions to assign to capture result.",
    )


class PipelineMonitorSettings(BaseSettings):
    """
    Settings to start a pipeline, monitor it, and capture the results when
    finished.
    """

    computation_polling_interval: int = Field(
        default=180,
        description=(
            "Time in seconds in between checks that the pipeline is finished."
        ),
        gte=5,
    )
    data_asset_ready_polling_interval: int = Field(
        default=10,
        description=(
            "Time in seconds in between checks that the captured data asset "
            "is ready."
        ),
        gte=5,
    )
    run_params: RunParams = Field(
        ..., description="Parameters for running a pipeline"
    )
    capture_settings: Optional[CaptureSettings] = Field(
        default=None,
        description=(
            "Optional field for capturing the results as an asset. If None, "
            "then will not capture results."
        ),
    )
