import aiohttp
import aiofiles


async def upload_zip(target_path, zip_path, token):
    """Upload a zip file to a target URL."""
    async with aiohttp.ClientSession(
        headers={"Authorization": f"Bearer {token}"}
    ) as session:
        async with aiofiles.open(zip_path, "rb") as f:
            data = aiohttp.FormData()
            data.add_field("file", await f.read(), filename="output.zip")
            async with session.post(target_path, data=data) as resp:
                if resp.status == 200:
                    return {"uploaded": True, "path": target_path}
                else:
                    raise Exception(f"Upload failed with status {resp.status}")
