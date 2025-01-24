from dataclasses import dataclass
from typing import List, Dict
from threading import Thread

from .tasks.base import Task, Status

class Workflow(Thread):
    def __init__(self, workflow_def: "WorkflowDef"):
        super().__init__()
        self.workflow_def = workflow_def

    def run(self):
        for task in self.workflow_def.tasks:
            assert task.status == Status.PENDING
            try:
                print(f"Doing {task.name}", self.workflow_def.status)
                task.status = Status.RUNNING
                task.run(context=self.workflow_def)
                task.status = Status.COMPLETED
            except Exception as e:
                print(f"Failed {task.name}", e)
                task.status = Status.FAILED
                task.detail = str(e)
                return

        for task in self.workflow_def.tasks:
            try:
                task.postrun(context=self.workflow_def)
            except Exception as e:
                print(f"Cleanup exception: {str(e)}. Continue to other cleanups.")

    @staticmethod
    def convert_ng(workflow_id: str, url: str, bucketname: str, prefix: str, token: str):
        from pathlib import Path
        from hashlib import md5

        from .tasks.common import (
            Download,
            DOWNLOAD_URL_KEY,
            DOWNLOAD_DST_PATH,
            DOWNLOAD_TOKEN_KEY,

            BucketUploadTask,
            UPLOAD_SRC_KEY,
            UPLOAD_DST_KEY,
            UPLOAD_BUCKET_KEY,
            UPLOAD_TOKEN_KEY,
        )
        from .tasks.convert_ng import (
            PATH_TO_NII_KEY,
            PATH_TO_DST_KEY,
            GenerateInfo,
            GenerateScales,
            GenerateHighestLod,
            ComputeScales,
        )

        root_output_dir = Path(".temp")
        root_output_dir.mkdir(parents=True, exist_ok=True)
        output_dir = root_output_dir / md5(url.encode("utf-8")).hexdigest()
        
        output_nii = str(output_dir.with_name("input" + "".join(Path(url).suffixes)))
        output_ng_precomp = str(output_dir / "precomputed")
        
        workflow_def = WorkflowDef(
            workflow_id=workflow_id,
            workflow_name="Converting neuroglancer",
            workflow_input={
                DOWNLOAD_URL_KEY: url,
                DOWNLOAD_DST_PATH: output_nii,
                PATH_TO_NII_KEY: output_nii,
                PATH_TO_DST_KEY: output_ng_precomp,
                UPLOAD_SRC_KEY: output_ng_precomp,
                UPLOAD_DST_KEY: prefix,
                UPLOAD_BUCKET_KEY: bucketname,
                UPLOAD_TOKEN_KEY: token,
            },
            tasks=[
                Download(),
                GenerateInfo(),
                GenerateScales(),
                GenerateHighestLod(),
                ComputeScales(),
                BucketUploadTask(),
            ])
        return Workflow(workflow_def=workflow_def)

@dataclass
class WorkflowDef(Thread):
    workflow_id: str
    workflow_name: str
    workflow_input: Dict
    tasks: List[Task]

    @property
    def status(self):
        for idx, task in enumerate(self.tasks):
            if task.status == Status.COMPLETED:
                continue
            if task.status in {Status.RUNNING, Status.PENDING}:
                return {
                    "status": Status.RUNNING.value,
                    "current_step": task.name,
                    "progress": idx * 100 // len(self.tasks)
                }
            if task.status == Status.FAILED:
                return {
                    "status": Status.FAILED.value,
                    "current_step": task.name,
                    "progress": idx * 100 // len(self.tasks)
                }
        return {
            "status": Status.COMPLETED.value,
            "current_step": None,
            "progress": 100
        }
