from django.db.models import signals
from django.db.models.loading import get_model

from haystack import indexes
from haystack import site
from haystack.utils import get_identifier

from tasks import search_index_update, search_index_delete


class QueuedSearchIndex(indexes.SearchIndex):
    """
    A ``SearchIndex`` subclass that enqueues updates for later processing.

    Deletes are handled instantly since a reference, not the instance, is put on the queue. It would not be hard
    to update this to handle deletes as well (with a delete task).
    """
    # We override the built-in _setup_* methods to connect the enqueuing operation.
    def _setup_save(self, model):
        signals.post_save.connect(self.enqueue_save, sender=model)

    def _setup_delete(self, model):
        signals.post_delete.connect(self.enqueue_delete, sender=model)

    def _teardown_save(self, model):
        signals.post_save.disconnect(self.enqueue_save, sender=model)

    def _teardown_delete(self, model):
        signals.post_delete.disconnect(self.enqueue_delete, sender=model)

    def enqueue_save(self, instance, **kwargs):
        search_index_update.delay(instance._meta.app_label, instance._meta.module_name, instance._get_pk_val())

    def enqueue_delete(self, instance, **kwargs):
        search_index_delete.delay(instance._meta.app_label, instance._meta.module_name, get_identifier(instance))
