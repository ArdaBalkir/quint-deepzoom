import aiohttp
import aiofiles
import os


async def get_download(path, token, task_id, task_instance):
    """Download a file from a URL asynchronously."""
    async with aiohttp.ClientSession(
        headers={"Authorization": f"Bearer {token}"}
    ) as session:
        async with session.get(path) as resp:
            if resp.status == 200:
                filename = f"/data/{task_id}_image"
                async with aiofiles.open(filename, "wb") as f:
                    await f.write(await resp.read())
                return filename
            else:
                raise Exception(f"Download failed with status {resp.status}")
