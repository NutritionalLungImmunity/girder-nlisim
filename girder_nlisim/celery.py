from celery import Celery

app = Celery('nli')
app.conf.update(
    imports=['girder_nlisim.tasks'], task_serializer='pickle', accept_content=['pickle']
)
