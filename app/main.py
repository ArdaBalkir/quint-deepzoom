from fastapi import FastAPI, Request, HTTPException
import pyvips, requests, os, zipfile

app = FastAPI()


async def get_download(path):
    # Verify token for the job initially somehow
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{path}"
    response = requests.get(url)

    if response.status_code == 200:
        os.makedirs("temp/downloads", exist_ok=True)
        filename = os.path.basename(path)
        filepath = os.path.join("temp/downloads", filename)

        with open(filepath, "wb") as file:
            file.write(response.content)

        return os.path.relpath(filepath)
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")


async def deepzoom(path):
    image = pyvips.Image.new_from_file(path)
    outputPath = os.path.abspath(os.path.join("temp", "outputs", path.split("\\")[-1]))
    image.dzsave(outputPath)
    outputPath += ".dzi"
    return outputPath


async def upload_zip(upload_path, zip_path, token):
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
            detail="Failed to upload file",
        )
    print(upload_response)
    if upload_response.status_code == 201:
        print(f"Created in {upload_path}")
        return f"Created in {upload_path}"
    else:
        return "non 201 repsonse"


async def zip_pyramid(path):
    dzi_file = path
    dzi_dir = os.path.splitext(dzi_file)[0] + "_files"

    strip_file_name = os.path.basename(os.path.splitext(dzi_file)[0])

    zip_path = f"{os.path.dirname(dzi_file)}/{strip_file_name}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:

        # Add the .dzi file
        zipf.write(dzi_file, os.path.basename(dzi_file))

        # Add all files from the associated directory
        # This should avoid memory issues hopefully
        for root, _, files in os.walk(dzi_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, os.path.dirname(dzi_dir))
                zipf.write(file_path, arcname)

    return zip_path


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/deepzoom")
async def deepzoom_endpoint(request: Request):
    data = await request.json()
    path = data.get("path")
    bucketname = data.get("bucketname")
    token = data.get("token")
    if not path:
        raise HTTPException(status_code=400, detail="Missing param in request body")

    # Where the job occurs and is made up of 4 subprocesses
    print(f"Starting download of {path}")
    response_path = await get_download(path)
    print(f"Downloaded file to {response_path}")

    print(f"Calling for DeepZoom")
    response_path = await deepzoom(response_path)

    # Token is only of importance for this upload
    print(f"Calling for Zip")
    zip_path = await zip_pyramid(response_path)

    upload_filename = os.path.basename(zip_path)

    res = await upload_zip(f"{bucketname}/zips/{upload_filename}", zip_path, token)
    # Once this is done use a cleanup to avoid running out of storage
    # TODO add a cleanup function to remove the temp

    return {"job_status": res}
