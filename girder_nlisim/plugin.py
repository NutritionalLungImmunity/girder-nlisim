from girder.plugin import getPlugin, GirderPlugin

from girder_nlisim.api import NLI


class NLIGirderPlugin(GirderPlugin):
    DISPLAY_NAME = 'NLI Simulation Runner'

    def load(self, info):
        getPlugin('jobs').load(info)
        info['apiRoot'].nli = NLI()
