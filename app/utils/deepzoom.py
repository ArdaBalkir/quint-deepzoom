import pyvips
import os

# import psutil


# Add psutil after initial testing
def deepzoom(download_path):
    """Convert an image to a DeepZoom pyramid."""
    # cpu_usage = psutil.cpu_percent(interval=1, percpu=True)
    # print(f"I spy with my little eye: {cpu_usage}% CPU cores")

    image = pyvips.Image.new_from_file(download_path)
    output_dir = f"{download_path}_files"
    os.makedirs(output_dir, exist_ok=True)
    image.dzsave(output_dir, suffix=".jpg")
    return output_dir
