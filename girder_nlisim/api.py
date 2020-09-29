from io import StringIO
from pathlib import Path

import attr
from girder.api import access
from girder.api.describe import autoDescribeRoute, Description
from girder.api.rest import filtermodel, Resource
from girder.constants import AccessType, SortDir
from girder.exceptions import RestException
from girder.models.folder import Folder
from girder_jobs.constants import JobStatus
from girder_jobs.models.job import Job

from girder_nlisim.models import Simulation
from girder_nlisim.tasks import GirderConfig, run_simulation
from nlisim.config import SimulationConfig

NLI_JOB_TYPE = 'nli_simulation'
NLI_CONFIG_FILE = Path(__file__).parent / 'nli-config.ini'


class NLI(Resource):
    def __init__(self):
        super().__init__()
        self.resourceName = 'nli'
        self.route('GET', ('job',), self.list_simulation_jobs)
        self.route('POST', ('job',), self.execute_simulation)

        self.route('GET', ('simulation',), self.list_simulations)
        self.route('POST', ('simulation',), self.create_simulation)
        self.route('POST', ('simulation', ':id', 'complete'), self.mark_simulation_complete)

    @access.user
    @filtermodel(Job)
    @autoDescribeRoute(
        Description('List running simulations associated with the current user')
        .pagingParams(defaultSort='created', defaultSortDir=SortDir.DESCENDING)
        .errorResponse()
    )
    def list_simulation_jobs(self, limit, offset, sort):
        user = self.getCurrentUser()
        job_model = Job()
        return job_model.list(
            types=[NLI_JOB_TYPE],
            statuses=[JobStatus.QUEUED, JobStatus.RUNNING],
            user=user,
            currentUser=user,
            limit=limit,
            offset=offset,
            sort=sort,
        )

    @access.user
    @filtermodel(Job)
    @autoDescribeRoute(
        Description('Run a simulation as an async task.')
        .param(
            'name',
            'The name of the simulation',
        )
        # TODO: What are the time units of the simulation
        .param(
            'targetTime',
            'The number of (hours?) to run the simulation',
            dataType='float',
        )
        .modelParam(
            'folderId',
            'The folder store simulation outputs in (defaults to the user\' "public" folder).',
            model=Folder,
            required=False,
            level=AccessType.WRITE,
        )
        .errorResponse()
        .errorResponse('Write access was denied on the folder.', 403)
    )
    def execute_simulation(self, name, targetTime, folder=None):
        user, token = self.getCurrentUser(returnToken=True)
        folder_model = Folder()
        job_model = Job()

        if folder is None:
            folder = folder_model.findOne(
                {'parentId': user['_id'], 'name': 'Public', 'parentCollection': 'user'}
            )
            if folder is None:
                raise RestException('Could not find the user\'s "public" folder.')

        girder_config = GirderConfig(
            api='http://localhost:8080/api/v1', token=str(token['_id']), folder=str(folder['_id'])
        )
        simulation_config = SimulationConfig(NLI_CONFIG_FILE)

        # TODO: This would be better stored as a dict, but it's easier once we change the
        #       config object format.
        simulation_config_file = StringIO()
        simulation_config.write(simulation_config_file)

        job = job_model.createJob(
            title='NLI Simulation',
            type=NLI_JOB_TYPE,
            kwargs={
                'girder_config': attr.asdict(girder_config),
                'simulation_config': simulation_config_file.getvalue(),
            },
            user=user,
        )

        run_simulation.delay(
            name=name,
            girder_config=girder_config,
            simulation_config=simulation_config,
            target_time=targetTime,
            job=job,
        )
        return job

    @access.public
    @filtermodel(Simulation)
    @autoDescribeRoute(
        Description('List simulations.')
        .pagingParams(defaultSort='created', defaultSortDir=SortDir.DESCENDING)
        .errorResponse()
    )
    def list_simulations(self, limit, offset, sort):
        user = self.getCurrentUser()
        simulation_model = Simulation()
        return simulation_model.list(user=user, limit=limit, offset=offset, sort=sort)

    @access.user
    @filtermodel(Simulation)
    @autoDescribeRoute(
        Description('Create a new simulation folder.')
        .param(
            'name',
            'The name of the simulation',
        )
        .modelParam(
            'folderId',
            'The folder containing the simulation.',
            model=Folder,
            level=AccessType.WRITE,
        )
        .jsonParam(
            'config',
            'The simulation configuration object.',
            requireObject=True,
        )
        .notes(
            'This endpoint should only be called by the simulation task. '
            'Use the `POST /job` endpoint to run a simulation.'
        )
        .errorResponse()
        .errorResponse('Write access was denied on the folder.', 403)
    )
    def create_simulation(self, name, config, folder, public=None):
        user = self.getCurrentUser()
        simulation_model = Simulation()
        return simulation_model.createSimulation(folder, name, config, user, public)

    @access.user
    @filtermodel(Simulation)
    @autoDescribeRoute(
        Description('Indicate that a simulation has completed successfully.')
        .modelParam(
            'id',
            'The simulation id.',
            model=Simulation,
            level=AccessType.WRITE,
            destName='simulation',
        )
        .notes('This endpoint should only be called by the simulation task.')
        .errorResponse()
        .errorResponse('Write access was denied on the simulation.', 403)
    )
    def mark_simulation_complete(self, simulation):
        simulation_model = Simulation()
        return simulation_model.setSimulationComplete(simulation)
