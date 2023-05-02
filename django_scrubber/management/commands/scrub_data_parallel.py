import concurrent.futures
import logging
import re
import shlex
import subprocess

from django.core.management.base import BaseCommand


logger = logging.getLogger(__name__)


def scrub_data(model):
    """Run scrub_data on the given model."""
    scrub_data = shlex.split(f'python manage.py scrub_data --model {model} --remove-fake-data')
    result = subprocess.run(scrub_data)
    return result.returncode


def scrub_validation():
    """Run scrub_validation and return its output."""
    scrub_validation = shlex.split('python manage.py scrub_validation')
    result = subprocess.run(scrub_validation, capture_output=True, text=True)
    return result.stdout


class Command(BaseCommand):

    def handle(self, *args, **options):
        models = []

        for line in scrub_validation().split('\n'):
            if match := re.search(r"^Model '([a-zA-Z0-9_.]+)'.*", line):
                models.append(match.group(1))

        logger.info(f'{len(models)} model(s) to be scrubbed.')

        with concurrent.futures.ProcessPoolExecutor() as executor:
            for model in models:
                logger.info(f'Scrubbing model {model}')
                future = executor.submit(scrub_data, model)
                future.scrub_model = model
                future.add_done_callback(lambda f: logger.info(f'Completed scrubbing of model {f.scrub_model}'))
