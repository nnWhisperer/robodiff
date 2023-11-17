from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from uuid import UUID

# Assuming the structure of JobRepository and its methods are implemented in Python
from job_manager import JobRepository

app = FastAPI()

# Instantiate the JobRepository
repo = JobRepository()

class JobSubmissionRecord(BaseModel):
    InnerSimulations: list
    JobDetails: dict

class ExportFolderName(BaseModel):
    folderName: str
    compression: Optional[bool] = None

@app.post("/register")
async def register(nodeDetails: dict):
    return repo.RegisterNode(nodeDetails=nodeDetails)

@app.post("/shutdownnode/{nodeGuid}")
async def shutdown_node(nodeGuid: UUID):
    repo.ShutdownNode(nodeGuid)
    return {"message": "OK"}

@app.get("/stats")
async def get_stats():
    return repo.GetStats()

@app.get("/exportnodes")
async def export_nodes():
    return repo.ExportNodes()

@app.post("/exportcompletedfolder")
async def export_completed_folder(folderInfo: ExportFolderName):
    repo.ExportCompletedToFolder(folderInfo.folderName, compression=folderInfo.compression)
    return {"message": "OK"}

@app.post("/exportallfolder")
async def export_all_folder(folderInfo: ExportFolderName):
    repo.ExportAllToFolder(folderInfo.folderName, compression=folderInfo.compression)
    return {"message": "OK"}

@app.post("/submitjob")
async def submit_job(job: JobSubmissionRecord):
    return repo.EnqueueJob(job.InnerSimulations, job.JobDetails)

@app.get("/simulation/{nodeGuid}")
async def get_simulation(nodeGuid: UUID):
    return repo.GetSimulation(nodeGuid)

@app.post("/results/{workGuid}")
async def post_results(workGuid: UUID, incrementalResults: dict):
    return repo.AppendResults(workGuid, incrementalResults)

@app.post("/finished/{workGuid}")
async def work_finished(workGuid: UUID):
    return repo.WorkFinished(workGuid)

@app.post("/errors/{workGuid}")
async def record_error(workGuid: UUID, errorDetails: Optional[dict] = None):
    return repo.RecordError(workGuid, errorDetails)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=24478)
