from django.db.models.loading import get_model

from haystack import site
from haystack.management.commands import update_index

from celery.task import Task, PeriodicTask
from celery.task import task
from celery.task.schedules import crontab

@task(default_retry_delay=5*60, max_retries=1, name='search.index.update')
def search_index_update(app_name, model_name, pk, **kwargs):
    logger = search_index_update.get_logger(**kwargs)
    try:
        model_class = get_model(app_name, model_name)
        instance = model_class.objects.get(pk=pk)
        search_index = site.get_index(model_class)
        search_index.update_object(instance)
    except Exception, exc:
        logger.error(exc)
        search_index_update.retry(exc=exc)

@task(default_retry_delay=5*60, max_retries=1, name='search.index.delete')
def search_index_delete(app_name, model_name, obj_identifier, **kwargs):
    logger = search_index_delete.get_logger(**kwargs)
    try:
        model_class = get_model(app_name, model_name)
        search_index = site.get_index(model_class)
        search_index.remove_object(obj_identifier)
    except Exception, exc:
        logger.error(exc)
        search_index_delete.retry(exc=exc)


class SearchIndexUpdatePeriodicTask(PeriodicTask):
    routing_key = 'periodic.search.update_index'
    run_every = crontab(hour=4, minute=0)

    def run(self, **kwargs):
        logger = self.get_logger(**kwargs)
        logger.info("Starting update index")
        # Run the update_index management command
        update_index.Command().handle()
        logger.info("Finishing update index")

