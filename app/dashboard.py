from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse
import os
import pathlib

app = FastAPI()


@app.get("/")
async def get_dashboard():
    """Serve the monitoring dashboard"""
    dashboard_path = os.path.join(
        pathlib.Path(__file__).parent.absolute(), "static", "dashboard.html"
    )
    if os.path.exists(dashboard_path):
        return FileResponse(dashboard_path)
    else:
        raise HTTPException(status_code=404, detail="Dashboard not found")
