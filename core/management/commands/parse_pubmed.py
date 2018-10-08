from django.core.management.base import BaseCommand, CommandError
from .parse_xml import Parser
import logging
from django.conf import settings

logger = logging.getLogger('pubmed')

class Command(BaseCommand):
    help = "Parse pubmed xml to db"

    def add_arguments(self, parser):
        parser.add_argument('file', nargs='+', type=str)
        parser.add_argument('--force', action='store_true', help='Force parse file, delete exists info from db')
    
    def handle(self, *args, **options):
        logger.debug(options)
        force = options['force']
        for file in options['file']:
            logger.info(f'Parse file {file}')
            p = Parser(file, force=force)

            