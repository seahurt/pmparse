from django.core.management.base import BaseCommand, CommandError
from .parse_xml import Parser
import logging

logger = logging.getLogger('pubmed')


class Command(BaseCommand):
    help = "Parse pubmed xml to db"

    def add_arguments(self, parser):
        parser.add_argument('file', nargs='+', type=str)
    
    def handle(self, *args, **options):
        logger.info('OK')
        logger.error('NOT OK')

            