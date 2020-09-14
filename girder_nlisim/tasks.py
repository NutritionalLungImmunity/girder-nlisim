from logging import getLogger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List
from urllib.request import urlopen

import attr
from celery import Task
from girder_client import GirderClient
from girder_jobs.constants import JobStatus
from girder_jobs.models.job import Job

from girder_nlisim.celery import app
from nlisim.config import SimulationConfig
from nlisim.postprocess import generate_vtk
from nlisim.solver import run_iterator, Status

logger = getLogger(__name__)
GEOMETRY_FILE_URL = (
    'https://data.nutritionallungimmunity.org/api/v1/file/5ebd86cec1b2cfe0661e681f/download'
)


@attr.s(auto_attribs=True, kw_only=True)
class GirderConfig:
    """Configure where the data from a simulation run is posted."""

    #: authentication token
    token: str

    #: root folder id where the data will be placed
    folder: str

    #: base api url
    api: str = 'https://data.nutritionallungimmunity.org/api/v1'

    @property
    def client(self) -> GirderClient:
        cl = GirderClient(apiUrl=self.api)
        cl.token = self.token
        return cl

    def upload(self, name: str, directory: Path) -> str:
        """Upload files to girder and return the created folder id."""
        client = self.client
        logger.info(f'Uploading to {name}')
        folder = client.createFolder(self.folder, name)['_id']
        for file in directory.glob('*'):
            self.client.uploadFileToFolder(folder, str(file))
        return folder


def download_geometry():
    geometry_file_path = Path('geometry.hdf5')
    if not geometry_file_path.is_file():
        with urlopen(GEOMETRY_FILE_URL) as f, geometry_file_path.open('wb') as g:
            g.write(f.read())


@app.task(bind=True)
def run_simulation(
    self: Task,
    girder_config: GirderConfig,
    simulation_config: SimulationConfig,
    target_time: float,
    job: Dict[str, Any],
) -> List[str]:
    """Run a simulation and export postprocessed vtk files to girder."""
    job_model = Job()

    def update_task_state(status: int):
        if not self.request.called_directly:
            meta = {
                'time_step': time_step,
                'current_time': state.time,
                'target_time': target_time,
                'folders': folders,
            }
            job['status'] = status
            job['meta'] = meta
            job_model.save(job)

    try:
        download_geometry()
        folders: List[str] = []
        time_step = 0

        for state, status in run_iterator(simulation_config, target_time):
            logger.info(f'Simulation time {state.time}')
            with TemporaryDirectory() as temp_dir:
                temp_dir_path = Path(temp_dir)
                generate_vtk(state, temp_dir_path)

                name = '%03i' % time_step if status != Status.finalize else 'final'
                folders.append(girder_config.upload(name, temp_dir_path))
                update_task_state(JobStatus.RUNNING)

            time_step += 1

        update_task_state(JobStatus.SUCCESS)
        return folders
    except Exception:
        update_task_state(JobStatus.ERROR)
        raise
