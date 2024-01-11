import time
import uuid
from typing import Any, List, Sequence
from airflow.models.baseoperator import BaseOperator
from social.hooks.facebook.page_post import FacebookPagePostHook
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat


class FacebookPageInsightsOperator(BaseOperator, GCSDataUpload):
    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
    )

    ui_color = "#3b5998"

    def __init__(
        self,
        http_conn_id: str,
        page_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        metrics: List[str],
        **kwargs,
    ):
        BaseOperator.__init__(self, **kwargs)
        GCSDataUpload.__init__(self, gcs_bucket, gcs_prefix, **kwargs)

        self.page_id = page_id
        self.metrics = metrics
        self.hook = FacebookPagePostHook(http_conn_id=http_conn_id)

    def execute(self, context: Any):
        ds = context["ds"]
        self._get_data_and_upload(ds)

    def _get_data_and_upload(self, ds):
        partition_prefix = f"page_id={self.page_id}/snapshot_date={ds}"
        collected_ts = round(time.time() * 1000)

        data = self.hook.get_page_insight(page_id=self.page_id, metrics=self.metrics, ds=ds)
        
        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )
