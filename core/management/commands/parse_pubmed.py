from django.core.management.base import BaseCommand, CommandError
from .parse_xml import Parser

class Command(BaseCommand):
    help = "Parse pubmed xml to db"

    def add_arguments(self, parser):
        parser.add_argument('file', nargs='+', type=str)
        parser.add_argument('--force', action='store_true', help='Force parse file, delete exists info from db')
    
    def handle(self, *args, **options):
        print(options)
        force = options['force']
        for file in options['file']:
            p = Parser(file, force=force)

            