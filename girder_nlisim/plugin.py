from girder import constants, events, logger
from girder.plugin import GirderPlugin, getPlugin
from girder_jobs.constants import JobStatus
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

        # update the total progress (defining this as the mean progress)
        experiment['nli']['progress'] = sum(per_sim_progress.values()) / len(per_sim_progress)

        # update job status
        experiment['nli']['per_sim_status'][str(simulation_id)] = job['status']
        # any errors or cancellations count as an error or cancellation of the experiment,
        # experiment doesn't become active until all of the sims are active.
        if any(
            status == JobStatus.ERROR for status in experiment['nli']['per_sim_status'].values()
        ):
            experiment['nli']['status'] = JobStatus.ERROR
        elif any(
            status == JobStatus.CANCELED for status in experiment['nli']['per_sim_status'].values()
        ):
            experiment['nli']['status'] = JobStatus.CANCELED
        elif any(
            status == JobStatus.INACTIVE for status in experiment['nli']['per_sim_status'].values()
        ):
            experiment['nli']['status'] = JobStatus.INACTIVE
        else:
            # in this case, all statuses must be QUEUED, RUNNING, or SUCCESS
            # we take the "minimum" for the experiment's status.
            if any(
                status == JobStatus.QUEUED
                for status in experiment['nli']['per_sim_status'].values()
            ):
                experiment['nli']['status'] = JobStatus.QUEUED
            elif any(
                status == JobStatus.RUNNING
                for status in experiment['nli']['per_sim_status'].values()
            ):
                experiment['nli']['status'] = JobStatus.RUNNING
            else:
                experiment['nli']['status'] = JobStatus.SUCCESS

        experiment_model.save(experiment)


class NLIGirderPlugin(GirderPlugin):
    DISPLAY_NAME = 'NLI Simulation Runner'

    def load(self, info):
        getPlugin('jobs').load(info)
        info['apiRoot'].nli = NLI()

        events.bind('jobs.job.update.after', 'nlisim', update_status)
        job_model = Job()
        job_model.exposeFields(level=constants.AccessType.ADMIN, fields={'args', 'kwargs'})
