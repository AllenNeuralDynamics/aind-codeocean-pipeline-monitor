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


class CapturedDataAssetParams(BaseSettings, DataAssetParams):
    """
    Make name and mount fields optional. They will be determined after the
    pipeline is finished.
    """

    # Override fields from DataAssetParams model
    name: Optional[str] = Field(default=None)
    mount: Optional[str] = Field(default=None)
    # Source will be determined after pipeline is finished
    source: Literal[None] = Field(default=None)

    # Additional fields
    input_data_name: Optional[str] = Field(default=None)
    process_name_suffix: Optional[str] = Field(default="processed")
    process_name_suffix_tz: Optional[str] = Field(default="UTC")
    data_description_location: Optional[str] = Field(
        default="/results/data_description.json"
    )
    permissions: Permissions = Field(
        default=Permissions(everyone=EveryoneRole.Viewer),
        description="Permissions to assign to capture result.",
    )


class PipelineMonitorSettings(BaseSettings):
    """
    Settings to start a pipeline, monitor it, and capture the results when
    finished.
    """

    run_params: RunParams = Field(
        ..., description="Parameters for running a pipeline"
    )
    captured_data_asset_params: Optional[CapturedDataAssetParams] = Field(
        default=None,
        description=(
            "Optional field for capturing the results as an asset. If None, "
            "then will not capture results."
        ),
    )
