import logging
import datetime

from django.conf import settings
from django.db.models import F, signals
from django.db.utils import IntegrityError, DataError
from django.core.exceptions import FieldDoesNotExist
from django.core.management.base import BaseCommand, CommandError
from django.apps import apps

from ... import settings_with_fallback

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Replace database data according to model-specific or global scrubbing rules.'
    leave_locale_alone = True

    def add_arguments(self, parser):
        parser.add_argument('--model', type=str, required=False,
                            help='Scrub only a single model (format <app_label>.<model_name>)')

    def handle(self, *args, **kwargs):
        if settings.ENVIRONMENT not in ['STAGING', 'DEVELOP'] :
            # avoid logger, otherwise we might silently fail if we're on live and logging is being sent somewhere else
            self.stderr.write('this command should only be run our staging env')
            return False

        global_scrubbers = settings_with_fallback('SCRUBBER_GLOBAL_SCRUBBERS')

        # run only for selected model
        if kwargs.get('model', None) is not None:
            app_label, model_name = kwargs.get('model').rsplit('.', 1)
            try:
                models = [apps.get_model(app_label=app_label, model_name=model_name)]
            except LookupError:
                raise CommandError('--model should be defined as <app_label>.<model_name>')

        # run for all models of all apps
        else:
            models = apps.get_models()

        for model in models:
            if model._meta.proxy:
                continue
            if settings_with_fallback('SCRUBBER_SKIP_UNMANAGED') and not model._meta.managed:
                continue
            if (settings_with_fallback('SCRUBBER_APPS_LIST') and
                    model._meta.app_config.name not in settings_with_fallback('SCRUBBER_APPS_LIST')):
                continue

            scrubbers = dict()
            for field in model._meta.fields:
                if field.name in global_scrubbers:
                    scrubbers[field] = global_scrubbers[field.name]
                elif type(field) in global_scrubbers:
                    scrubbers[field] = global_scrubbers[type(field)]

            scrubbers.update(_get_model_scrubbers(model))

            if not scrubbers:
                continue

            realized_scrubbers = _filter_out_disabled(_call_callables(scrubbers))

            logger.info('Scrubbing %s with %s', model._meta.label, realized_scrubbers)

            options = dict(_get_options(model))

            if 'disconnect_signals' in options:
                disconnect_signals = options['disconnect_signals']

                for signal_data in disconnect_signals:
                    model_signal = getattr(signals, signal_data['type'])
                    model_signal.disconnect(
                        sender=signal_data['sender'],
                        dispatch_uid=signal_data['dispatch_uid'],
                    )

            if 'trim_table' in options:
                filter_kwargs = {options['trim_attribute'] + '__gte': datetime.datetime.now() - datetime.timedelta(days=30)}
                delete_queryset = model.objects.filter(**filter_kwargs)

                _large_delete(delete_queryset, model)
            
            records = model.objects.all()

            if 'exclude' in options:
                records = records.exclude(**options['exclude'])

            try:
                records.annotate(
                    mod_pk=F('pk') % settings_with_fallback('SCRUBBER_ENTRIES_PER_PROVIDER')
                ).update(**realized_scrubbers)
            except IntegrityError as e:
                raise CommandError('Integrity error while scrubbing %s (%s); maybe increase '
                                   'SCRUBBER_ENTRIES_PER_PROVIDER?' % (model, e))
            except DataError as e:
                raise CommandError('DataError while scrubbing %s (%s)' % (model, e))


def _call_callables(d):
    """
    Helper to realize lazy scrubbers, like Faker, or global field-type scrubbers
    """
    return {k.name: (callable(v) and v(k) or v) for k, v in d.items()}

def _get_options(model):
    try:
        options = model.Scrubbers.Meta
    except AttributeError:
        return {}
    
    return _get_fields(options)

def _large_delete(queryset, model):
    counter = 0
    slice_step = 1000
    count = queryset.count()
    iterations = int(count / slice_step)

    while counter < iterations:
        slice_start = counter * slice_step
        slice_end = (counter + 1) * slice_step
        logger.info('Deleting model {} {} {}'.format(model, slice_start, slice_end))
        ids = queryset.values_list('id', flat=True)[slice_start:slice_end]
        model.objects.filter(id__in=ids).delete()
        counter += 1
    
    queryset.delete()

def _get_model_scrubbers(model):
    scrubbers = dict()
    try:
        scrubber_cls = getattr(model, 'Scrubbers')
    except AttributeError:
        return scrubbers  # no model-specific scrubbers

    for k, v in _get_fields(scrubber_cls):
        if k == 'Meta':
            continue
        try:
            field = model._meta.get_field(k)
        except FieldDoesNotExist as e:
            raise

        scrubbers[field] = v

    return scrubbers


def _get_fields(d):
    """
    Helper to get "normal" (i.e.: non-magic and non-dunder) instance attributes.
    Returns an iterator of (field_name, field) tuples.
    """
    return ((k, v) for k, v in vars(d).items() if not k.startswith('_'))


def _filter_out_disabled(d):
    """
    Helper to remove Nones (actually any false-like type) from the scrubbers.
    This is needed so we can disable global scrubbers in a per-model basis.
    """
    return {k: v for k, v in d.items() if v}
