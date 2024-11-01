from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pyvips
import os
import zipfile
import aiohttp
import aiofiles
import asyncio
from aiofiles.os import makedirs, remove, path
import shutil

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# TODO Threaded test for performance
# TODO Add origins
# TODO Add token and user validation


async def get_download(path, token):
    """
    Asynchronous definition for downloading the file from Ebrains hosted bucket
    """
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{path}?redirect=false"
    headers = {"Authorization": f"Bearer {token}"}

    async with aiohttp.ClientSession() as session:
        # Get the download URL
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                download_url = data.get("url")
                if not download_url:
                    raise Exception("Download URL not provided in response")

                await makedirs("temp/downloads", exist_ok=True)
                filename = os.path.basename(path)
                filepath = os.path.join("temp/downloads", filename)

                # Stream the download asynchronously
                async with session.get(download_url) as download_response:
                    if download_response.status == 200:
                        async with aiofiles.open(filepath, "wb") as file:
                            async for chunk in download_response.content.iter_chunked(
                                8192
                            ):
                                await file.write(chunk)
                        return os.path.relpath(filepath)
                    else:
                        raise Exception(
                            f"Failed to download file. Status code: {download_response.status}"
                        )
            else:
                raise Exception(
                    f"Failed to get download URL. Status code: {response.status}"
                )


async def deepzoom(path):
    """
    Creates a DeepZoom pyramid from the image file specified
    Runs in a thread pool since pyvips operations are CPU-bound
    """

    def process_image():
        image = pyvips.Image.new_from_file(path)
        output_dir = os.path.abspath(os.path.join("temp", "outputs"))
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, os.path.basename(path))
        image.dzsave(output_path)
        return output_path + ".dzi"

    # Run CPU-intensive task in a thread pool
    return await asyncio.to_thread(process_image)


async def upload_zip(upload_path, zip_path, token):
    """
    Asynchronous definition for uploading zip file to EBrains hosted bucket
    """
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{upload_path}"
    headers = {"Authorization": f"Bearer {token}"}

    async with aiohttp.ClientSession() as session:
        # Get the upload URL
        async with session.put(url, headers=headers) as response:
            if response.status != 200:
                raise HTTPException(
                    status_code=response.status, detail="Failed to get upload URL"
                )
            data = await response.json()
            upload_url = data.get("url")
            if not upload_url:
                raise HTTPException(
                    status_code=400, detail="Upload URL not provided in response"
                )

            print(f"Uploading to {upload_url}")
            async with aiofiles.open(zip_path, "rb") as file:
                file_data = await file.read()
                async with session.put(upload_url, data=file_data) as upload_response:
                    if upload_response.status == 201:
                        print(f"Created in {upload_path}")
                        return f"Created in {upload_path}"
                    else:
                        raise HTTPException(
                            status_code=upload_response.status,
                            detail="Failed to upload file",
                        )


async def zip_pyramid(path):
    """
    Function zips the pyramid files with a .dzip extension
    Runs in a thread pool since compression is CPU-bound
    """

    def create_zip():
        dzi_file = path
        dzi_dir = os.path.splitext(dzi_file)[0] + "_files"
        strip_file_name = os.path.basename(os.path.splitext(dzi_file)[0])
        zip_path = f"{os.path.dirname(dzi_file)}/{strip_file_name}.dzip"

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(dzi_file, os.path.basename(dzi_file))
            for root, _, files in os.walk(dzi_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, os.path.dirname(dzi_dir))
                    zipf.write(file_path, arcname)
        return zip_path

    # Run CPU-intensive task in a thread pool
    return await asyncio.to_thread(create_zip)


async def cleanup_files(download_path, dzi_path, zip_path):
    """
    Asynchronously remove temporary files after processing
    """
    try:
        if await path.exists(download_path):
            await remove(download_path)

        if dzi_path and await path.exists(dzi_path):
            await remove(dzi_path)
            dzi_dir = os.path.splitext(dzi_path)[0] + "_files"
            if await path.exists(dzi_dir):
                # Run rmtree in a thread pool since it's filesystem intensive
                await asyncio.to_thread(shutil.rmtree, dzi_dir)

        if zip_path and await path.exists(zip_path):
            await remove(zip_path)

    except Exception as e:
        print(f"Cleanup error: {str(e)}")


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/deepzoom")
async def deepzoom_endpoint(request: Request):
    data = await request.json()

    print(f"Received a request")

    path = data.get("path")
    target_path = data.get("target_path")
    token = data.get("token")

    # Parameter validation
    missing_params = []
    if not path:
        missing_params.append("path")
    if not target_path:
        missing_params.append("target_path")
    if not token:
        missing_params.append("token")

    if missing_params:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required parameters: {', '.join(missing_params)}",
        )

    if not isinstance(path, str) or not path.strip():
        raise HTTPException(status_code=400, detail="Path must be a non-empty string")
    if not isinstance(target_path, str) or not target_path.strip():
        raise HTTPException(
            status_code=400, detail="Target path must be a non-empty string"
        )
    if not isinstance(token, str) or not token.strip():
        raise HTTPException(status_code=400, detail="Token must be a non-empty string")

    try:
        print(f"Starting download of {path}")
        download_path = await get_download(path, token)
        print(f"Downloaded file to {download_path}")

        print(f"DeepZoom process started for {download_path}")
        dzi_path = await deepzoom(download_path)
        print(f"DeepZoom process completed for {download_path}")

        print(f"Zip process started for {dzi_path}")
        zip_path = await zip_pyramid(dzi_path)
        print(f"Zip process completed for {dzi_path}")

        upload_filename = os.path.basename(zip_path)
        res = await upload_zip(f"{target_path}/{upload_filename}", zip_path, token)

        print("Upload status: ", res)

        await cleanup_files(
            os.path.join("temp/downloads", os.path.basename(path)), dzi_path, zip_path
        )

        return {"job_status": res}

    except Exception as e:
        await cleanup_files(
            os.path.join("temp/downloads", os.path.basename(path)),
            dzi_path if "dzi_path" in locals() else None,
            zip_path if "zip_path" in locals() else None,
        )
        raise HTTPException(status_code=500, detail=str(e))
