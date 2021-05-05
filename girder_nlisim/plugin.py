import girder
from girder import constants, events, logger
from girder.plugin import getPlugin, GirderPlugin
from girder_jobs.models.job import Job

from girder_nlisim.api import NLI, NLI_JOB_TYPE
from girder_nlisim.models import Experiment, Simulation


def update_status(event):
    simulation_model = Simulation()
    job = event.info['job']
    if job['type'] != NLI_JOB_TYPE:
        return

    simulation_id = job['kwargs'].get('simulation_id')
    simulation = simulation_model.load(simulation_id, force=True)

    if simulation is None:
        logger.error(f'Could not find simulation for job {job["_id"]}')
        return

    progress = job['progress']
    simulation['nli']['progress'] = 100 * (progress['current'] / progress['total'])
    simulation['nli']['status'] = job['status']
    simulation_model.save(simulation)

    # update the progress for the experiment, if this is part of one
    if job['kwargs'].get('in_experiment'):
        experiment_model = Experiment()
        experiment = experiment_model.load(job['kwargs'].get('experiment_id'), force=True)

        # update the individual progress
        experiment['nli']['per_sim_progress'][str(simulation_id)] = simulation['nli']['progress']
        per_sim_progress = experiment['nli']['per_sim_progress']

        # update the total progress (=average progress)
        experiment['nli']['progress'] = sum(per_sim_progress.values()) / len(per_sim_progress)

        experiment_model.save(experiment)


class NLIGirderPlugin(GirderPlugin):
    DISPLAY_NAME = 'NLI Simulation Runner'

    def load(self, info):
        getPlugin('jobs').load(info)
        info['apiRoot'].nli = NLI()

        events.bind('jobs.job.update.after', 'nlisim', update_status)
        job_model = Job()
        job_model.exposeFields(level=constants.AccessType.ADMIN, fields={'args', 'kwargs'})
