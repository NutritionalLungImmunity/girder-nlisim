from girder.constants import AccessType
from girder.models.folder import Folder
from girder_jobs.constants import JobStatus


class Simulation(Folder):
    def initialize(self):
        self._skipNLIFilter = False
        super(Simulation, self).initialize()
        self.ensureIndices(['nli.complete', 'nli.creator'])
        self.exposeFields(level=AccessType.READ, fields=('nli',))

    def createSimulation(self, parentFolder, name, config, creator, public=None):
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
                'status': JobStatus.INACTIVE,
            }
        finally:
            self._skipNLIFilter = False
        return self.save(folder)

    def setSimulationComplete(self, simulation):
        simulation.get('nli', {})['complete'] = True
        return self.save(simulation)

    def find(self, query=None, **kwargs):
        query = query or {}
        if not self._skipNLIFilter:
            query['nli.complete'] = {'$exists': True}
        return super(Simulation, self).find(query, **kwargs)

    def findOne(self, query=None, **kwargs):
        query = query or {}
        if not self._skipNLIFilter:
            query['nli.complete'] = {'$exists': True}
        return super(Simulation, self).findOne(query, **kwargs)

    def list(self, includeArchived=False, **kwargs):
        query = {}
        if not includeArchived:
            query = {
                'nli.archived': {'$ne': True},
            }
        return self.findWithPermissions(query, **kwargs)
