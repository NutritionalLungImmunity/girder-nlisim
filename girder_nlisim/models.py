import itertools
from typing import Dict, List, Tuple

from girder.constants import AccessType
from girder.models.folder import Folder
from girder_jobs.constants import JobStatus


class Simulation(Folder):
    def initialize(self):
        # noinspection PyAttributeOutsideInit
        self._skipNLIFilter = False
        super(Simulation, self).initialize()
        self.ensureIndices(['nli.complete', 'nli.creator'])
        self.exposeFields(level=AccessType.READ, fields=('nli',))

    def createSimulation(
        self, *, parentFolder, name, config, creator, version, public=None, experiment=None
    ):
        # This is an ugly way to bypass the custom filter for nlisimulations in the folder
        # listing.  Otherwise, when creating a new folder there are duplicate names.  I
        # don't see a better way around this other than intercept the default folder
        # query logic at a higher level.
        self._skipNLIFilter = True
        try:
            folder = super(Simulation, self).createFolder(
                parentFolder, name, public=public, creator=creator, allowRename=True
            )
            folder['nli'] = {
                'complete': False,
                'config': config,
                'author': f'{creator["firstName"]} {creator["lastName"]}',
                'archived': False,
                'progress': 0,
                'version': version,
                'status': JobStatus.INACTIVE,
                'simulation': True,
                'in_experiment': (experiment is not None),
                'experiment_id': None if experiment is None else experiment['_id'],
            }
            super(Simulation, self).setMetadata(
                folder=folder, metadata={'simulation': True, 'config': config}
            )
        finally:
            self._skipNLIFilter = False
        return self.save(folder)

    def setSimulationComplete(self, simulation):
        simulation.get('nli', {})['complete'] = True
        return self.save(simulation)

    def get_summary_stats(self, simulation, user) -> Dict[str, Dict]:
        """Creates the summary statistics of a simulation in json form."""
        # I'm just going to assume that all subfolders are for time-steps but I'll skip them
        # if they don't have a time field set. (or, horrors, if it is negative)
        stats = dict()

        self._skipNLIFilter = True
        # comments in the girder internals indicate that eager evaluation is better here,
        # as there can be time outs
        subfolders = list(
            super(Simulation, self).childFolders(simulation, parentType='folder', user=user)
        )
        self._skipNLIFilter = False
        for folder in subfolders:
            time = folder['meta'].get('time', -1)
            if time < 0:
                continue
            stats[time] = folder['meta'].get('nli', {})

        return stats

    def find(self, query=None, **kwargs):
        query = query or {}
        # Sometimes we need to search all folders e.g. to avoid name conflicts, other times
        # just for simulation folders. We can just check simulation folders by seeing if
        # the nli.simulation field is set
        if not self._skipNLIFilter:
            query['nli.simulation'] = {'$exists': True}
        return super(Simulation, self).find(query, **kwargs)

    def findOne(self, query=None, **kwargs):
        query = query or {}
        # Sometimes we need to search all folders e.g. to avoid name conflicts, other times
        # just for simulation folders. We can just check simulation folders by seeing if
        # the nli.simulation field is set
        if not self._skipNLIFilter:
            query['nli.simulation'] = {'$exists': True}
        return super(Simulation, self).findOne(query, **kwargs)

    def list(self, includeArchived=False, creator=None, config=None, in_experiment=False, **kwargs):
        query = {}
        if not includeArchived:
            query = {
                'nli.archived': {'$ne': True},
            }
        if in_experiment:
            query['nli.in_experiment'] = {'$eq': True}
        if creator:
            query['creatorId'] = creator['_id']
        if config:
            query.update(**self.filter_by_config(config))
        return self.findWithPermissions(query, **kwargs)

    @classmethod
    def filter_by_config(cls, config):
        query = {}  # type: ignore
        for c in config:
            key = f'nli.config.{c["module"]}.{c["key"]}'
            query[key] = {}
            min, max = c['range']
            if min is not None:
                query[key]['$gte'] = min
            if max is not None:
                query[key]['$lte'] = max
        return query


class Experiment(Folder):
    def initialize(self):
        self._skipNLIFilter = False
        super(Experiment, self).initialize()
        self.ensureIndices(['nli.creator'])
        self.exposeFields(level=AccessType.READ, fields=('nli',))

    def createExperiment(
        self,
        parentFolder,
        name,
        config,
        creator,
        version,
        experimental_variables,
        runs_per_config,
        public=None,
    ):
        # This is an ugly way to bypass the custom filter for nlisimulations in the folder
        # listing.  Otherwise, when creating a new folder there are duplicate names.  I
        # don't see a better way around this other than intercept the default folder
        # query logic at a higher level.
        self._skipNLIFilter = True
        try:
            folder = super(Experiment, self).createFolder(
                parentFolder, name, public=public, creator=creator, allowRename=True
            )
            folder['nli'] = {
                'config': config,
                'experimental_variables': experimental_variables,
                'author': f'{creator["firstName"]} {creator["lastName"]}',
                'archived': False,
                'component_simulations': [],
                'progress': 0,
                'per_sim_progress': dict(),
                'version': version,
                'per_sim_status': dict(),
                'status': JobStatus.INACTIVE,
                'experiment': True,
            }
            super(Experiment, self).setMetadata(
                folder=folder,
                metadata={
                    "experiment": True,
                    "experimental variables": experimental_variables,
                    "runs per config": runs_per_config,
                    'config': config,
                },
            )

        finally:
            self._skipNLIFilter = False
        return self.save(folder)

    def find(self, query=None, **kwargs):
        query = query or {}
        # Sometimes we need to search all folders e. g. to avoid folder name conflicts, other
        # times just for simulation folders. We can just check simulation folders by seeing
        # if the nli.experiment field is set
        if not self._skipNLIFilter:
            query['nli.experiment'] = {'$exists': True}
        return super(Experiment, self).find(query, **kwargs)

    def findOne(self, query=None, **kwargs):
        query = query or {}
        # Sometimes we need to search all folders e. g. to avoid folder name conflicts, other
        # times just for simulation folders. We can just check simulation folders by seeing
        # if the nli.experiment field is set
        if not self._skipNLIFilter:
            query['nli.experiment'] = {'$exists': True}
        return super(Experiment, self).findOne(query, **kwargs)

    def list(self, includeArchived=False, creator=None, experimental_variables=None, **kwargs):
        query = {}
        if not includeArchived:
            query = {
                'nli.archived': {'$ne': True},
            }
        if creator:
            query['creatorId'] = creator['_id']
        if experimental_variables:
            query.update(**self.filter_by_experimental_variables(experimental_variables))
        return self.findWithPermissions(query, **kwargs)

    @classmethod
    def filter_by_experimental_variables(cls, experimental_variables: List[Tuple[str, str, list]]):
        # TODO: find out how to do a query in girder, possibly restructure storage of
        #  experimental variables
        return {}

    def get_summary_stats(self, experiment, user) -> Dict[str, Dict]:
        """Creates the summary statistics of an experiment in json form."""
        experiment_complete = True
        experimental_variables = experiment['meta']['experimental variables']
        runs_per_config = experiment['meta']['runs per config']

        # form a list of experimental groups, each simulation will be in one of these
        # param_names is a list of (module name, parameter name) tuples where each is
        # one of the experimental variables
        param_names = [
            (experimental_variable['module'], experimental_variable['parameter'])
            for experimental_variable in experimental_variables
        ]

        # experimental_group_params is a list of experimental treatments, where each experimental
        # treatment is encoded as a tuple of 3-tuples (module, param, value) specifying the
        # assignment of values to the experimental variables
        experimental_group_params = [
            tuple(
                (module, param, value) for (module, param), value in zip(param_names, param_values)
            )
            for param_values in itertools.product(
                *(
                    experimental_variable['values']
                    for experimental_variable in experimental_variables
                )
            )
        ]

        simulation_model = Simulation()
        completion = dict()
        stats = dict()
        names = dict()
        groups = dict()

        self._skipNLIFilter = True
        # comments in the girder internals indicate that eager evaluation is better here,
        # as there can be time outs
        subfolders = list(
            super(Experiment, self).childFolders(experiment, parentType='folder', user=user)
        )
        self._skipNLIFilter = False
        for folder in subfolders:
            # sanity check, that this is the right kind of folder
            if (
                not folder['nli']
                or not folder['nli']['simulation']
                or not folder['nli']['in_experiment']
            ):
                continue

            names[str(folder['_id'])] = folder['name']

            # an experiment is complete iff all of its simulations are complete
            completion[str(folder['_id'])] = folder['nli']['complete']
            experiment_complete = experiment_complete and completion[str(folder['_id'])]

            # record which group this belongs to
            for group_num, group_params in enumerate(experimental_group_params):
                # for debugging, pre-seed with an error term which should be overwritten below
                groups[str(folder['_id'])] = -1

                if all(
                    folder['nli']['config'][module][parameter] == value
                    for module, parameter, value in group_params
                ):
                    groups[str(folder['_id'])] = group_num
                    break

            # record the actual stats
            stats[str(folder['_id'])] = simulation_model.get_summary_stats(folder, user)

        return {
            'experimental_group_params': experimental_group_params,
            'experiment_complete': experiment_complete,
            'names': names,
            'runs_per_config': runs_per_config,
            'simulation completion': completion,
            'simulation group map': groups,
            'stats': stats,
        }
