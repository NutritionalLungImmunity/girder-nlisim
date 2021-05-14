from typing import Dict, List, Tuple

import girder
from girder.constants import AccessType
from girder.models.folder import Folder
from girder_jobs.constants import JobStatus


class Simulation(Folder):
    def initialize(self):
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

    def get_summary_stats(self, simulation) -> Dict[int, Dict]:
        """Creates the summary statistics of a simulation in json form"""
        # I'm just going to assume that all subfolders are for time-steps but I'll skip them
        # if they don't have a time field set. (or, horrors, if it is set to -1)
        stats = dict()
        subfolders = super(Simulation, self).childFolders(simulation, 'folder')
        for folder in subfolders:
            time = folder.get('time', -1)
            if time == -1:
                continue
            stats[time] = folder.get('nli', {})

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
        # TODO: find out how to do a query in girder, possibly restucture storage of
        #  experimental variables
        return {}
