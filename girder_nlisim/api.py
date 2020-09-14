from io import StringIO

import attr
from girder.api import access
from girder.api.describe import autoDescribeRoute, Description
from girder.api.rest import Resource
from girder.constants import AccessType
from girder.models.folder import Folder
from girder_jobs.models.job import Job

from girder_nlisim.tasks import GirderConfig, run_simulation
from nlisim.config import SimulationConfig


class NLI(Resource):
    def __init__(self):
        super().__init__()
        self.resourceName = 'nli'

    @access.user
    @autoDescribeRoute(
        Description('Run a simulation as an async task.')
        .param('folderId', 'The folder store simulation outputs in')
        # TODO: What are the time units of the simulation
        .param('targetTime', 'The number of (hours?) to run the simulation', dataType='float')
        .errorResponse()
        .errorResponse('Write access was denied on the folder.', 403)
    )
    def execute_simulation(self, folderId, targetTime):
        user, token = self.getCurrentUser(returnToken=True)
        folder_model = Folder()
        job_model = Job()

        folder = folder_model.load(folderId, user=user, level=AccessType.WRITE, exc=True)
        girder_config = GirderConfig(token=token['_id'], folder=folder['_id'])
        simulation_config = SimulationConfig()

        # TODO: This would be better stored as a dict, but it's easier once we change the
        #       config object format.
        simulation_config_file = StringIO()
        simulation_config.write(simulation_config_file)

        job = job_model.createJob(
            title='NLI Simulation',
            type='nli_simulation',
            kwargs={
                'girder_config': attr.asdict(girder_config),
                'simulation_config': simulation_config_file.getvalue(),
            },
        )

        run_simulation.delay(
            girder_config=girder_config,
            simulation_config=simulation_config,
            target_time=targetTime,
            job=job,
        )
        return job
