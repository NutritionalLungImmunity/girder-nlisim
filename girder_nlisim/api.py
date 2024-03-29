import copy
import csv
import io
import itertools
import math
import os
from io import StringIO
from logging import getLogger
from pathlib import Path
from typing import Any, Dict, List

import attr
from girder.api import access, rest
from girder.api.describe import Description, autoDescribeRoute
from girder.api.rest import Resource, filtermodel
from girder.constants import AccessType, SortDir
from girder.exceptions import RestException
from girder.models.folder import Folder
from girder.models.user import User
from girder_jobs.constants import JobStatus
from girder_jobs.models.job import Job
from nlisim import __version__ as nlisim_version
from nlisim.config import SimulationConfig

from girder_nlisim.models import Experiment, Simulation
from girder_nlisim.tasks import GirderConfig, run_simulation

logger = getLogger(__name__)

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


def simulation_runner(
    *,
    config,
    parent_folder,
    job_model: Job,
    run_name,
    target_time,
    token,
    user,
    experiment=None,
):
    simulation_model = Simulation()
    simulation = simulation_model.createSimulation(
        parentFolder=parent_folder,
        name=run_name,
        config=config,
        creator=user,
        version=nlisim_version,
        public=True,
        experiment=experiment,
    )

    # if this is to be part of an experiment, let the experiment know about it
    if experiment is not None:
        experiment['nli']['component_simulations'].append(simulation['_id'])
        experiment['nli']['per_sim_progress'][str(simulation['_id'])] = 0.0
        experiment['nli']['per_sim_status'][str(simulation['_id'])] = JobStatus.INACTIVE
        experiment_model = Experiment()
        experiment_model.save(experiment)

    girder_config = GirderConfig(
        api=GIRDER_API, token=str(token['_id']), folder=str(parent_folder['_id'])
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
            'in_experiment': (experiment is not None),
            'experiment_id': None if experiment is None else experiment['_id'],
        },
        user=user,
    )

    simulation['nli']['job_id'] = job['_id']
    simulation_model.save(simulation)
    run_simulation.delay(
        name=run_name,
        girder_config=girder_config,
        simulation_config=simulation_config,
        target_time=target_time,
        job=job,
        simulation_id=simulation['_id'],
    )
    return job, simulation


def flatten_dict(d, prefix=None):
    """Recursively flatten a dictionary to a dotted format.

    e.g.
    d = {'a': {'b':1, 'c':2}, 'd': {'b': 3, 'c':4 }}
    flatten(d) -> [('a.b', 1), ('a.c', 2), ('d.b', 3), ('d.c', 4)]
    """
    prefix = list() if prefix is None else prefix
    result = []
    for key, value in d.items():
        if isinstance(value, dict):
            result.extend(flatten_dict(value, prefix=[*prefix, key]))
        else:
            result.append(('.'.join([*prefix, key]), value))
    return result


class NLI(Resource):
    def __init__(self):
        super().__init__()
        self.resourceName = 'nli'
        self.route('GET', ('job',), self.list_simulation_jobs)
        self.route('POST', ('job',), self.execute_simulation)

        self.route('POST', ('experiment',), self.run_experiment)
        self.route('GET', ('experiment',), self.list_experiments)
        self.route('GET', ('experiment', ':id'), self.get_experiment)
        self.route('GET', ('experiment', ':id', 'csv'), self.get_experiment_csv)
        self.route('GET', ('experiment', ':id', 'json'), self.get_experiment_json)
        self.route('POST', ('experiment', ':id', 'cancel'), self.cancel_experiment)

        self.route('GET', ('simulation',), self.list_simulations)
        self.route('GET', ('simulation', ':id'), self.get_simulation)
        self.route('POST', ('simulation', ':id', 'complete'), self.mark_simulation_complete)
        self.route('POST', ('simulation', ':id', 'archive'), self.mark_simulation_archived)
        self.route('GET', ('simulation', ':id', 'csv'), self.get_simulation_csv)
        self.route('GET', ('simulation', ':id', 'json'), self.get_simulation_json)
        self.route('POST', ('simulation', ':id', 'cancel'), self.cancel_simulation)

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
        target_time = config.get('simulation', {}).get('run_time', 96)
        user, token = self.getCurrentUser(returnToken=True)
        folder_model: Folder = Folder()
        job_model: Job = Job()

        if folder is None:
            folder = folder_model.findOne(
                {'parentId': user['_id'], 'name': 'Public', 'parentCollection': 'user'}
            )
            if folder is None:
                raise RestException('Could not find the user\'s "public" folder.')

        job, simulation = simulation_runner(
            config=config,
            parent_folder=folder,
            job_model=job_model,
            run_name=name,
            target_time=target_time,
            token=token,
            user=user,
        )

        return job

    @access.user
    @filtermodel(Job)
    @autoDescribeRoute(
        Description('Run an experiment (series of simulations) as a collection of async tasks.')
        .param(
            'name',
            'The name of the experiment',
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
    def run_experiment(self, name, config, folder=None):
        target_time = config.get('simulation', {}).get('run_time', 0)
        # if there is no run_time, this is not a valid request
        if target_time <= 0:
            raise RestException('Invalid (or unprovided) run time for experiment.')

        runs_per_config = config.get('simulation', {}).get('runs_per_config', 1)
        max_run_digit_len = math.floor(1 + math.log10(runs_per_config))

        user, token = self.getCurrentUser(returnToken=True)
        folder_model = Folder()
        job_model = Job()

        if folder is None:
            folder = folder_model.findOne(
                {'parentId': user['_id'], 'name': 'Public', 'parentCollection': 'user'}
            )
            if folder is None:
                raise RestException('Could not find the user\'s "public" folder.')

        # for each of the configuration values which are lists, we run the simulator with
        # each of the possible values. (cartesian product)
        configs = [dict()]
        experimental_variables: List[Dict[str, Any]] = []
        for module, module_config in config.items():
            for parameter, parameter_values in module_config.items():
                if isinstance(parameter_values, list):
                    # this will unpack lists appropriately, even of length 0 or 1,
                    # but those are not experimental variables
                    if len(parameter_values) > 1:
                        experimental_variables.append(
                            {'module': module, 'parameter': parameter, 'values': parameter_values}
                        )
                    new_configs = []
                    for cfg in configs:
                        for val in parameter_values:
                            new_cfg = copy.deepcopy(cfg)
                            if module not in new_cfg:
                                new_cfg[module] = dict()
                            new_cfg[module][parameter] = val
                            new_configs.append(new_cfg)
                    configs = new_configs
                else:
                    for cfg in configs:
                        if module not in cfg:
                            cfg[module] = dict()
                        cfg[module][parameter] = parameter_values
        # create a folder to hold the various runs of the simulator
        # TODO: what if this fails? how does it fail?
        experiment_model = Experiment()
        experiment_folder = experiment_model.createExperiment(
            parentFolder=folder,
            name=name,
            config=config,
            creator=user,
            version=nlisim_version,
            experimental_variables=experimental_variables,
            runs_per_config=runs_per_config,
            public=True,
        )
        jobs = []

        for config_variant in configs:
            for run_number in range(runs_per_config):
                # create an informative name for the run, noting the run number and the values
                # of the experimental variables
                run_name = name + "-run-" + str(run_number).zfill(max_run_digit_len)
                for experimental_variable in experimental_variables:
                    run_name += (
                        '-'
                        + str(experimental_variable['module'])
                        + "."
                        + str(experimental_variable['parameter'])
                        + "-"
                        + str(
                            config_variant[experimental_variable['module']][
                                experimental_variable['parameter']
                            ]
                        )
                    )

                job, simulation = simulation_runner(
                    config=config_variant,
                    parent_folder=experiment_folder,
                    job_model=job_model,
                    run_name=run_name,
                    target_time=target_time,
                    token=token,
                    user=user,
                    experiment=experiment_folder,
                )
                jobs.append(job)

        return jobs

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
        .param(
            'experiments',
            "Include simulations that are part of an experiment",
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
        self, limit, offset, sort, includeArchived, mine, experiments, creator=None, config=None
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
            in_experiment=experiments,
        )

    @access.public
    @filtermodel(Experiment)
    @autoDescribeRoute(
        Description('List experiments.')
        .param(
            'includeArchived',
            'Include archived experiments in the list.',
            dataType='boolean',
            default=False,
        )
        .param(
            'mine',
            "Only include the current user's experiments",
            dataType='boolean',
            default=False,
        )
        .modelParam(
            'creator',
            'Only list experiments from the given user',
            model=User,
            level=AccessType.READ,
            required=False,
            paramType='query',
            destName='creator',
        )
        .pagingParams(defaultSort='created', defaultSortDir=SortDir.DESCENDING)
        .errorResponse()
    )
    def list_experiments(self, limit, offset, sort, includeArchived, mine, creator=None):
        user = self.getCurrentUser()
        experiment_model = Experiment()
        if mine and user is None:
            return []
        if mine and creator and creator['_id'] != user['_id']:
            return []
        if mine:
            creator = user
        return experiment_model.list(
            includeArchived=includeArchived,
            user=user,
            limit=limit,
            offset=offset,
            sort=sort,
            creator=creator,
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

    @access.public
    @filtermodel(Experiment)
    @autoDescribeRoute(
        Description('Get an experiment.')
        .modelParam(
            'id',
            'The experiment id.',
            model=Experiment,
            level=AccessType.READ,
            destName='experiment',
        )
        .errorResponse()
    )
    def get_experiment(self, experiment):
        return experiment

    @access.user
    @rest.rawResponse
    @autoDescribeRoute(
        Description('Get the statistics of an experiment in csv format.')
        .modelParam(
            'id',
            'The experiment id.',
            model=Experiment,
            level=AccessType.READ,
            destName='experiment',
        )
        .errorResponse()
    )
    def get_experiment_csv(self, experiment):
        user = self.getCurrentUser()
        experiment_model = Experiment()
        experiment_stats = experiment_model.get_summary_stats(experiment, user)

        # collect time steps, in order
        time_steps = list(
            set(
                time
                for sim_id, sim_data in experiment_stats['stats'].items()
                for time, stats in sim_data.items()
            )
        )
        time_steps.sort(key=lambda x: float(x))  # string ordering != float ordering

        # create a natural ordering for the simulations by their experimental variables
        simulation_ids = list(experiment_stats['simulation config'].keys())
        simulation_ids.sort(
            key=lambda ident: flatten_dict(experiment_stats['simulation config'][ident])
        )

        num_simulations = len(simulation_ids)

        # bail with empty response, if no data
        if len(time_steps) <= 0 or num_simulations <= 0:
            return ''

        per_sim_variables = [
            var_name
            for var_name, var_value in flatten_dict(
                experiment_stats['stats'][simulation_ids[0]][time_steps[0]]
            )
        ]
        num_vars = len(per_sim_variables)

        with io.StringIO() as sio:
            csvwriter = csv.writer(sio, dialect='excel')

            # write out a pre-header to list experimental parameters
            csvwriter.writerow(['Simulation Name', 'Parameter', 'Value'])
            for sim_id in simulation_ids:
                for n, (param_name, param_value) in enumerate(
                    flatten_dict(experiment_stats['simulation config'][sim_id])
                ):
                    csvwriter.writerow(
                        [
                            experiment_stats['names'][sim_id] if n == 0 else '',
                            param_name,
                            param_value,
                        ]
                    )

            # write the names of the simulations as a higher level header
            csvwriter.writerow(
                ['']
                + list(
                    itertools.chain.from_iterable(
                        [experiment_stats['names'][sim_id]] + (num_vars - 1) * ['']
                        for sim_id in simulation_ids
                    )
                )
            )

            # another header, each sim has the same set of variables, which get repeated for each
            csvwriter.writerow(["time"] + num_simulations * per_sim_variables)

            # now to actually write the data. note that not all simulations may have all time steps
            # as this could be called while the simulations are still running
            for time_step in time_steps:
                csvwriter.writerow(
                    [float(time_step)]
                    + list(
                        itertools.chain.from_iterable(
                            [
                                var_value
                                for var_name, var_value in flatten_dict(
                                    experiment_stats['stats'][sim_id][time_step]
                                )
                            ]
                            if time_step in experiment_stats['stats'][sim_id]
                            else num_vars * ['']
                            for sim_id in simulation_ids
                        )
                    )
                )

            return sio.getvalue()

    @access.user
    @rest.rawResponse
    @autoDescribeRoute(
        Description('Get the statistics of a simulation in csv format.')
        .modelParam(
            'id',
            'The simulation id.',
            model=Simulation,
            level=AccessType.READ,
            destName='simulation',
        )
        .errorResponse()
    )
    def get_simulation_csv(self, simulation):
        user = self.getCurrentUser()
        simulation_model = Simulation()
        summary_stats = simulation_model.get_summary_stats(simulation, user)

        # The values of summary stats will typically be nested dicts, now we flatten them
        summary_stats = {time: flatten_dict(data) for time, data in summary_stats.items()}
        # move it to a list and sort by time
        summary_stats = [(time, data) for time, data in summary_stats.items()]
        summary_stats.sort(key=lambda x: float(x[0]))

        # write a csv to memory
        with io.StringIO() as sio:
            csvwriter = csv.writer(sio, dialect='excel')
            if len(summary_stats) > 0:
                # header
                csvwriter.writerow(["time", *[label for label, value in summary_stats[0][1]]])

                for time, data in summary_stats:
                    csvwriter.writerow([time, *[value for label, value in data]])

            rest.setResponseHeader('Content-Type', 'text/csv')
            return sio.getvalue()

    @access.user
    @autoDescribeRoute(
        Description('Get the statistics of an experiment in json format.')
        .modelParam(
            'id',
            'The experiment id.',
            model=Experiment,
            level=AccessType.READ,
            destName='experiment',
        )
        .errorResponse()
    )
    def get_experiment_json(self, experiment):
        user = self.getCurrentUser()
        experiment_model = Experiment()
        return experiment_model.get_summary_stats(experiment, user)

    @access.user
    @autoDescribeRoute(
        Description('Get the statistics of a simulation in json format.')
        .modelParam(
            'id',
            'The simulation id.',
            model=Simulation,
            level=AccessType.READ,
            destName='simulation',
        )
        .errorResponse()
    )
    def get_simulation_json(self, simulation):
        user = self.getCurrentUser()
        simulation_model = Simulation()
        return simulation_model.get_summary_stats(simulation, user)

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

    @access.user
    @filtermodel(Simulation)
    @autoDescribeRoute(
        Description('Cancel a simulation.')
        .modelParam(
            'id',
            'The simulation id.',
            model=Simulation,
            level=AccessType.WRITE,
            destName='simulation',
        )
        .errorResponse()
        .errorResponse('Write access was denied on the simulation.', 403)
    )
    def cancel_simulation(self, simulation):
        self._cancel_simulation(simulation)

    @access.user
    @filtermodel(Simulation)
    @autoDescribeRoute(
        Description('Cancel an experiment.')
        .modelParam(
            'id',
            'The experiment id.',
            model=Experiment,
            level=AccessType.READ,
            destName='experiment',
        )
        .errorResponse()
        .errorResponse('Write access was denied on the experiment.', 403)
    )
    def cancel_experiment(self, experiment):
        simulation_model = Simulation()
        for simulation in simulation_model.childFolders(experiment['_id'], 'folder'):
            # TODO: I added the 'folder' so that the signature matches. This was my best guess;
            #  'folder', 'user', 'collection' are the available options.
            # noinspection PyBroadException
            try:
                self._cancel_simulation(simulation)
            except Exception:
                logger.exception(f'Failed to cancel simulation "{simulation["_id"]}"')

    def _cancel_simulation(self, simulation):
        job_model = Job()
        job_id = simulation['nli']['job_id']
        job = job_model.load(job_id, force=True)
        job_model.cancelJob(job)
