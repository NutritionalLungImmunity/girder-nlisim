from io import StringIO
import os
from pathlib import Path

import attr
from girder.api import access
from girder.api.describe import autoDescribeRoute, Description
from girder.api.rest import filtermodel, Resource
from girder.constants import AccessType, SortDir
from girder.exceptions import RestException
from girder.models.folder import Folder
from girder.models.user import User
from girder_jobs.constants import JobStatus
from girder_jobs.models.job import Job

from girder_nlisim.models import Simulation
from girder_nlisim.tasks import GirderConfig, run_simulation
from nlisim.config import SimulationConfig

NLI_JOB_TYPE = 'nli_simulation'
NLI_CONFIG_FILE = Path(__file__).parent / 'nli-config.ini'
GIRDER_API = os.environ.get('GIRDER_API', 'https://data.nutritionallungimmunity.org/api/v1').rstrip(
    '/'
)

config_filter_schema = {
    'title': 'ConfigFilter',
    'type': 'array',
    'items': {'$ref': '#/definitions/Config'},
    'definitions': {
        'Config': {
            'title': 'Config',
            'type': 'object',
            'properties': {
                'module': {'title': 'Module', 'type': 'string'},
                'key': {'title': 'Key', 'type': 'string'},
                'range': {
                    'title': 'Range',
                    'type': 'array',
                    'items': [{'type': ['number', 'null']}, {'type': ['number', 'null']}],
                },
            },
            'required': ['module', 'key', 'range'],
        }
    },
}


class NLI(Resource):
    def __init__(self):
        super().__init__()
        self.resourceName = 'nli'
        self.route('GET', ('job',), self.list_simulation_jobs)
        self.route('POST', ('job',), self.execute_simulation)

        self.route('GET', ('simulation',), self.list_simulations)
        self.route('GET', ('simulation', ':id'), self.get_simulation)
        self.route('POST', ('simulation', ':id', 'complete'), self.mark_simulation_complete)
        self.route('POST', ('simulation', ':id', 'archive'), self.mark_simulation_archived)

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
        .jsonParam('config', 'Simulation configuration', paramType='body', requireObject=True)
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
    def execute_simulation(self, name, config, folder=None):
        target_time = config.get('simulation', {}).get('run_time', 50)
        user, token = self.getCurrentUser(returnToken=True)
        folder_model = Folder()
        job_model = Job()

        if folder is None:
            folder = folder_model.findOne(
                {'parentId': user['_id'], 'name': 'Public', 'parentCollection': 'user'}
            )
            if folder is None:
                raise RestException('Could not find the user\'s "public" folder.')

        simulation_model = Simulation()
        simulation = simulation_model.createSimulation(
            folder,
            name,
            config,
            user,
            True,
        )
        girder_config = GirderConfig(
            api=GIRDER_API, token=str(token['_id']), folder=str(folder['_id'])
        )
        simulation_config = SimulationConfig(NLI_CONFIG_FILE, config)

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
                'config': config,
                'simulation_id': simulation['_id'],
            },
            user=user,
        )

        simulation['nli']['job_id'] = job['_id']
        simulation_model.save(simulation)

        run_simulation.delay(
            name=name,
            girder_config=girder_config,
            simulation_config=simulation_config,
            target_time=target_time,
            job=job,
            simulation_id=simulation['_id'],
        )
        return job

    @access.public
    @filtermodel(Simulation)
    @autoDescribeRoute(
        Description('List simulations.')
        .param(
            'includeArchived',
            'Include archived simulations in the list.',
            dataType='boolean',
            default=False,
        )
        .param(
            'mine',
            "Only include the current user's simulations",
            dataType='boolean',
            default=False,
        )
        .modelParam(
            'creator',
            'Only list simulations from the given user',
            model=User,
            level=AccessType.READ,
            required=False,
            paramType='query',
            destName='creator',
        )
        .jsonParam(
            'config',
            'Filter by configuration value',
            paramType='query',
            schema=config_filter_schema,
            required=False,
        )
        .pagingParams(defaultSort='created', defaultSortDir=SortDir.DESCENDING)
        .errorResponse()
    )
    def list_simulations(
        self, limit, offset, sort, includeArchived, mine, creator=None, config=None
    ):
        user = self.getCurrentUser()
        simulation_model = Simulation()
        if mine and user is None:
            return []
        if mine and creator and creator['_id'] != user['_id']:
            return []
        if mine:
            creator = user
        return simulation_model.list(
            includeArchived=includeArchived,
            user=user,
            limit=limit,
            offset=offset,
            sort=sort,
            creator=creator,
            config=config,
        )

    @access.public
    @filtermodel(Simulation)
    @autoDescribeRoute(
        Description('Get a simulation.')
        .modelParam(
            'id',
            'The simulation id.',
            model=Simulation,
            level=AccessType.READ,
            destName='simulation',
        )
        .errorResponse()
    )
    def get_simulation(self, simulation):
        return simulation

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

    @access.user
    @filtermodel(Simulation)
    @autoDescribeRoute(
        Description('Archive a simulation.')
        .modelParam(
            'id',
            'The simulation id.',
            model=Simulation,
            level=AccessType.WRITE,
            destName='simulation',
        )
        .param(
            'archived',
            'The archive state.',
            dataType='boolean',
            default=True,
        )
        .errorResponse()
        .errorResponse('Write access was denied on the simulation.', 403)
    )
    def mark_simulation_archived(self, simulation, archived):
        simulation['nli']['archived'] = archived
        simulation_model = Simulation()
        return simulation_model.save(simulation)
