import pytest
from girder.plugin import loadedPlugins


@pytest.mark.plugin('nli')
def test_import(server):
    assert 'nli' in loadedPlugins()
