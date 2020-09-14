from celery import Celery

app = Celery('nli')
app.conf.update(
    imports=['nli_simulation_runner.tasks'], task_serializer='pickle', accept_content=['pickle']
)
