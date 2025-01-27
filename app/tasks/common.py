import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from .base import Task

UPLOAD_SRC_KEY = "common:upload:src"
UPLOAD_DST_KEY = "common:upload:dst"
UPLOAD_BUCKET_KEY = "common:upload:bucket"
UPLOAD_TOKEN_KEY = "common:upload:token"

class BucketUploadTask(Task):
    name = "Upload"

    def run(self, *args, context, **kwargs):
        srcfilepath = context.workflow_input.get(UPLOAD_SRC_KEY)
        dstpath = context.workflow_input.get(UPLOAD_DST_KEY)
        bucket = context.workflow_input.get(UPLOAD_BUCKET_KEY)
        token = context.workflow_input.get(UPLOAD_TOKEN_KEY)

        assert srcfilepath, f"{UPLOAD_SRC_KEY} needs to be populated"
        assert dstpath, f"{UPLOAD_DST_KEY} needs to be populated"
        assert bucket, f"{UPLOAD_BUCKET_KEY} needs to be populated"
        assert token, f"{UPLOAD_TOKEN_KEY} needs to be populated"

        assert isinstance(dstpath, str)

        import requests

        session = requests.Session()

        def upload(_srcfilepath: str, _dstpath: str):
            url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{bucket}/{_dstpath}"
            headers = {"Authorization": f"Bearer {token}"}

            # Get the upload URL
            resp = session.put(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            
            upload_url = data.get("url")
            assert upload_url

            fp = open(_srcfilepath, "rb")
            resp = session.put(upload_url, data=fp)
            resp.raise_for_status()

        src = Path(srcfilepath)

        if src.is_file():
            upload(str(src), dstpath)
            return

        if src.is_dir():
            
            dst_root = dstpath.rstrip("/") + "/"
            args = [(str(f), dst_root + str(f.relative_to(src)))
                    for f in src.glob("**/*")
                    if f.is_file()]
            with ThreadPoolExecutor(max_workers=6) as ex:
                ex.map(
                    upload,
                    map(lambda a: a[0], args),
                    map(lambda a: a[1], args),
                )
            return
        raise TypeError(f"{dstpath} is neither file nor directory")

DOWNLOAD_URL_KEY = "common:download:url"
DOWNLOAD_DST_PATH = "common:download:dst"
DOWNLOAD_TOKEN_KEY = "common:download:token"
DOWNLOAD_HDR_KEY = "common:download:header"

class Download(Task):
    name = "Download"

    def postrun(self, *args, context, **kwargs):
        dst = context.workflow_input.get(DOWNLOAD_DST_PATH)
        if not dst:
            return
        os.unlink(dst)
    
    def run(self, *args, context, **kwargs):
        url = context.workflow_input.get(DOWNLOAD_URL_KEY)
        dst = context.workflow_input.get(DOWNLOAD_DST_PATH)
        token = context.workflow_input.get(DOWNLOAD_TOKEN_KEY)
        _headers = context.workflow_input.get(DOWNLOAD_HDR_KEY)

        assert url, f"{DOWNLOAD_URL_KEY} must be populated"
        assert dst, f"{DOWNLOAD_DST_PATH} must be populated"

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if _headers:
            headers = {
                **headers,
                **_headers,
            }


        import requests
        resp = requests.get(url, stream=True)
        with open(dst, "wb") as fp:
            for chunk in resp.iter_content(512):
                fp.write(chunk)
