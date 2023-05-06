import concurrent.futures
import logging
import re
import shlex
import subprocess

from django.core.management.base import BaseCommand


logger = logging.getLogger(__name__)


def scrub_data(model, older_than, trim_only):
    scrub_data = shlex.split(f'python manage.py scrub_data --model {model} --older-than {older_than} --remove-fake-data {"--trim-only" if trim_only else ""}')
    result = subprocess.run(scrub_data)
    return result.returncode


def scrub_validation():
    scrub_validation = shlex.split('python manage.py scrub_validation')
    result = subprocess.run(scrub_validation, capture_output=True, text=True)
    return result.stdout


class Command(BaseCommand):
    help = 'Run scrub_data in parallel.'

    def add_arguments(self, parser):
        parser.add_argument('--older-than', type=int, required=False, default=1095,
                            help='Trim tables older than this number of days. Defaults to 1095 (3 years).')
        parser.add_argument('--trim-only', action='store_true', required=False,
                            help='Trim only. No scrub. Consider doing this before scrubbing.')

    def handle(self, *args, **kwargs):
        older_than = kwargs.get('older_than')
        trim_only = kwargs.get('trim_only')
        models = []

        for line in scrub_validation().split('\n'):
            if match := re.search(r"^Model '([a-zA-Z0-9_.]+)'.*", line):
                models.append(match.group(1))

        logger.info(f'{len(models)} model(s) to be scrubbed. Trimming those older than {older_than} days.')

        with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
            for model in models:
                logger.info(f'Scrubbing model {model}')
                future = executor.submit(scrub_data, model, older_than, trim_only)
                future.scrub_model = model
                future.add_done_callback(lambda f: logger.info(f'Finished scrubbing model {f.scrub_model}'))
