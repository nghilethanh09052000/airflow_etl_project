import uuid
import time
from typing import Any, Sequence, Dict, List
from airflow.models.operator import BaseOperator
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
import requests
import logging


"""
    Spreadsheet: https://docs.google.com/spreadsheets/d/19mPUpj1kmUynbKQCU8YMQIr3d7RkgnGmNIfoEV58nDU/edit#gid=0
    Titok : https://www.tiktok.com/@playsipher/

    Please check the Tiktok Channel and append new video id and url on the spreadsheet before upload
"""

class TiktokVideo(
    BaseOperator,
    GCSDataUpload
):
    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
        "ds",
    )

    def __init__(
            self,
            task_id: str,
            gcs_bucket: str,
            gcs_prefix: str,
            gcp_conn_id: str,
            ds: str,
            **kwargs
        ):

        BaseOperator.__init__(
            self, 
            task_id = task_id,
            **kwargs
        )

        GCSDataUpload.__init__(
            self, 
            gcs_bucket = gcs_bucket,
            gcs_prefix = gcs_prefix, 
            **kwargs
        )
        self.ds = ds
        self.hook = GSheetsHook(
            gcp_conn_id=gcp_conn_id
        )

    def execute(self, context: Any):
        videos: List[Dict[str, str]] = self._get_spreadsheet_data()
        for video in videos:
            self._get_data(video)

        logging.info('Uploading Titok Video Successfully')

    def _get_spreadsheet_data(self) ->  List[Dict[str, str]]:

        columns = self.hook.get_values(
            spreadsheet_id='19mPUpj1kmUynbKQCU8YMQIr3d7RkgnGmNIfoEV58nDU',
            range_= 'A2:B1000',
            major_dimension='COLUMNS'
        )
        return [
            {
                'video_id': video_id,
                'url': url
            } for video_id, url in zip(columns[0], columns[1])
        ]

    def _get_data(
            self,
            video
        ):
        response = requests.get(
            url = 'https://api16-normal-c-useast1a.tiktokv.com/aweme/v1/feed/',
            params={
                'aweme_id': video.get('video_id')
            }
        )
        result = response.json()
        details = next((item for item in result.get('aweme_list') if item.get('aweme_id') == video.get('video_id')), None)

        if not details:
            logging.info(f'No data for {video.get("video_id")} in URL {video.get("url")} move to next step ')
            return

        statistics = details.get('statistics')
        data = {
            'video_id'             : str(video.get('video_id', None)),
            'url'                  : str(video.get('url', None)),
            'create_time'          : str(details.get('create_time', None)),
            'desc'                 : str(details.get('desc', None)),
            'user_digged'          : str(details.get('user_digged', None)),
            'rate'                 : str(details.get('rate', None)),
            'is_top'               : str(details.get('is_top', None)),
            'duration'             : str(details.get('duration', None)),
            'comment_count'        : str(statistics.get('comment_count', None)),
            'digg_count'           : str(statistics.get('digg_count', None)),
            'download_count'       : str(statistics.get('download_count', None)),
            'play_count'           : str(statistics.get('play_count', None)),
            'share_count'          : str(statistics.get('share_count', None)),
            'forward_count'        : str(statistics.get('forward_count', None)),
            'lose_count'           : str(statistics.get('lose_count', None)),
            'lose_comment_count'   : str(statistics.get('lose_comment_count', None)),
            'whatsapp_share_count' : str(statistics.get('whatsapp_share_count', None)),
            'collect_count'        : str(statistics.get('collect_count', None)),

        }
        self._upload_data(data=data)

    def _upload_data(self, data):
        collected_ts = round(time.time() * 1000)
        current_date = time.strftime("%Y-%m-%d")
        current_hour = time.strftime("%H")
        partition_prefix = f"snapshot_date={self.ds}"

        self.upload(
            object_name=f"/{partition_prefix}/{uuid.uuid4()}",
            data=data,
            gcs_file_format=SupportedGcsFileFormat.PARQUET,
            pre_upload_callable=self._prepare_before_upload(collected_ts),
        )
