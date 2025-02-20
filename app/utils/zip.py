import zipfile
import os


def zip_pyramid(dzi_path):
    """Zip the DeepZoom pyramid directory."""
    zip_path = f"{dzi_path}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(dzi_path):
            for file in files:
                zf.write(
                    os.path.join(root, file),
                    arcname=os.path.relpath(os.path.join(root, file), dzi_path),
                )
    return zip_path
