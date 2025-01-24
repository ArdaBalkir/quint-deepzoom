from pathlib import Path
import shutil
from abc import ABC

from .base import Task

PATH_TO_NII_KEY = "ng:src"
PATH_TO_DST_KEY = "ng:dst"
DOWNSCALE_METHOD_KEY = "ng:downscale_method"

class NgTask(Task, ABC):
    name = "ng task generic"
    def postrun(self, *args, context, **kwargs):
        dst = context.workflow_input.get(PATH_TO_DST_KEY)
        if not dst:
            return
        shutil.rmtree(dst)

class GenerateInfo(NgTask):
    name = "generate info"
    def run(self, *args, context, **kwargs):
        assert PATH_TO_NII_KEY in context.workflow_input, f"Expected {PATH_TO_NII_KEY} in input, but was not found"
        assert PATH_TO_DST_KEY in context.workflow_input, f"Expected {PATH_TO_DST_KEY} in input, but was not found"
        from neuroglancer_scripts.volume_reader import volume_file_to_info
        volume_file_to_info(
            context.workflow_input[PATH_TO_NII_KEY],
            context.workflow_input[PATH_TO_DST_KEY],
        )


class GenerateScales(NgTask):
    name = "generate scales"
    def run(self, *args, context, **kwargs):
        assert PATH_TO_DST_KEY in context.workflow_input, f"Expected {PATH_TO_DST_KEY} in input, but was not found"
        from neuroglancer_scripts.scripts.generate_scales_info import generate_scales_info
        
        dst = Path(context.workflow_input[PATH_TO_DST_KEY])
        generate_scales_info(str(dst / "info_fullres.json"), str(dst))
        

class GenerateHighestLod(NgTask):
    name = "generate highest log"
    def run(self, *args, context, **kwargs):
        assert PATH_TO_NII_KEY in context.workflow_input, f"Expected {PATH_TO_NII_KEY} in input, but was not found"
        assert PATH_TO_DST_KEY in context.workflow_input, f"Expected {PATH_TO_DST_KEY} in input, but was not found"

        from neuroglancer_scripts.volume_reader import volume_file_to_precomputed

        nii = context.workflow_input[PATH_TO_NII_KEY]
        dst = context.workflow_input[PATH_TO_DST_KEY]
        volume_file_to_precomputed(
            nii,
            dst,
            options={
                "flat": True,
                "gzip": False
            })


class ComputeScales(NgTask):
    name = "compute scales"
    def run(self, *args, context, **kwargs):
        assert PATH_TO_DST_KEY in context.workflow_input, f"Expected {PATH_TO_DST_KEY} in input, but was not found"
        
        downscale_method = context.workflow_input.get(DOWNSCALE_METHOD_KEY, "average")
        from neuroglancer_scripts.scripts.compute_scales import compute_scales
        compute_scales(
            context.workflow_input[PATH_TO_DST_KEY],
            downscale_method,
            options={
                "flat": True,
                "gzip": False,
            })
