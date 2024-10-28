from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pyvips, requests, os, zipfile

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# TODO Document the API
# TODO Document usage logs
# TODO Cache shortlived user information
# TODO Restrict origins


async def get_download(path, token):
    """
    Definition for downloading the file from Ebrains hosted bucket
    """
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{path}?redirect=false"
    headers = {"Authorization": f"Bearer {token}"}

    # Get the download URL
    response = requests.get(url, headers=headers)

    print(response.status_code)
    print(response.json())

    if response.status_code == 200:
        download_url = response.json().get("url")
        if not download_url:
            raise Exception("Download URL not provided in response")

        os.makedirs("temp/downloads", exist_ok=True)
        filename = os.path.basename(path)
        filepath = os.path.join("temp/downloads", filename)

        download_response = requests.get(download_url)
        if download_response.status_code == 200:
            with open(filepath, "wb") as file:
                file.write(download_response.content)
            return os.path.relpath(filepath)
        else:
            raise Exception(
                f"Failed to download file. Status code: {download_response.status_code}, Response: {download_response.json()}"
            )
    else:
        raise Exception(
            f"Failed to get download URL. Status code: {response.status_code}, Response: {response.json()}"
        )


async def deepzoom(path):
    """
    Creates a DeepZoom pyramid from the image file specified
    """
    image = pyvips.Image.new_from_file(path)
    output_dir = os.path.abspath(os.path.join("temp", "outputs"))
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, os.path.basename(path))
    # This is the main process
    # TODO add user submitted optional params in the future
    image.dzsave(output_path)
    output_path += ".dzi"
    return output_path


async def upload_zip(upload_path, zip_path, token):
    """
    Definition for uploading zip file to EBrains hosted bucket
    """
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{upload_path}"
    headers = {"Authorization": f"Bearer {token}"}

    # Get the upload URL
    response = requests.put(url, headers=headers)
    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code, detail="Failed to get upload URL"
        )

    upload_url = response.json().get("url")
    if not upload_url:
        raise HTTPException(
            status_code=400, detail="Upload URL not provided in response"
        )

    print(f"Uploading to {upload_url}")
    # Upload logs here
    with open(zip_path, "rb") as file:
        upload_response = requests.put(upload_url, data=file)
    if upload_response.status_code != 201:
        raise HTTPException(
            status_code=upload_response.status_code,
            detail="Failed to get upload link",
        )
    print(upload_response)
    if upload_response.status_code == 201:
        print(f"Created in {upload_path}")
        return f"Created in {upload_path}"
    else:
        raise HTTPException(
            status_code=upload_response.status_code,
            detail="Failed to upload file",
        )


async def zip_pyramid(path):
    """
    Function zips the pyramid files with a .dzip extension
    """
    dzi_file = path
    dzi_dir = os.path.splitext(dzi_file)[0] + "_files"

    strip_file_name = os.path.basename(os.path.splitext(dzi_file)[0])

    zip_path = f"{os.path.dirname(dzi_file)}/{strip_file_name}.dzip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:

        # Add the .dzi file
        zipf.write(dzi_file, os.path.basename(dzi_file))

        # Add all files from the associated directory
        # This should avoid memory issues
        for root, _, files in os.walk(dzi_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, os.path.dirname(dzi_dir))
                zipf.write(file_path, arcname)

    return zip_path


import shutil


async def cleanup_files(download_path, dzi_path, zip_path):
    """
    Remove temporary files after processing
    Passed the paths within the function to avoid unintended deletes
    """
    try:
        if os.path.exists(download_path):
            os.remove(download_path)

        if os.path.exists(dzi_path):
            os.remove(dzi_path)
            dzi_dir = os.path.splitext(dzi_path)[0] + "_files"
            if os.path.exists(dzi_dir):
                shutil.rmtree(dzi_dir)

        if os.path.exists(zip_path):
            os.remove(zip_path)

    except Exception as e:
        print(f"Cleanup error: {str(e)}")


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/deepzoom")
async def deepzoom_endpoint(request: Request):
    data = await request.json()

    # TODO Add user details to each job

    # Validate all required parameters
    path = data.get("path")
    target_path = data.get("target_path")
    token = data.get("token")

    # Check for missing parameters
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

    # Validate parameter formats
    if not isinstance(path, str):
        raise HTTPException(status_code=400, detail="Path must be a string")
    if not isinstance(target_path, str):
        raise HTTPException(status_code=400, detail="Target path must be a string")
    if not isinstance(token, str):
        raise HTTPException(status_code=400, detail="Token must be a string")

    # Additional path validation
    if not path.strip():
        raise HTTPException(status_code=400, detail="Path cannot be empty")
    if not target_path.strip():
        raise HTTPException(status_code=400, detail="Traget path cannot be empty")
    if not token.strip():
        raise HTTPException(status_code=400, detail="Token cannot be empty")

    # TODO Implement a token validation

    # Error handling for the full endpoint process
    try:
        # TODO
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
        # Ensuring in the event of any errors, there are no leftover files
        await cleanup_files(
            os.path.join("temp/downloads", os.path.basename(path)),
            dzi_path if "dzi_path" in locals() else None,
            zip_path if "zip_path" in locals() else None,
        )
        raise HTTPException(status_code=500, detail=str(e))
