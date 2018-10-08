import xmltodict
from core.models import Article, Source
import logging
from multiprocessing import Pool, dummy
from django.conf import settings
import hashlib
import os
from pathlib import Path
import time
import collections
import datetime
import sys


logger = logging.getLogger('pubmed')

def timeit(method):
    def timed(*args, **kw):
        logger.info(f'Start {method.__name__}')
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        logger.info(f'End {method.__name__}, using {round(te - ts, 2)}s')
        return result
    return timed

class Parser(object):
    month_map = {
        'Jan': '1',
        'Feb': '2',
        'Mar': '3',
        'Apr': '4',
        'May': '5',
        'Jun': '6',
        'Jul': '7',
        'Aug': '8',
        'Sep': '9',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }
    def __init__(self, file, force=False):
        self.file = Path(file).resolve()
        self.force = force
        self.path = str(self.file)
        self.filename = self.file.name
        self.calc_md5()
        self.filesize = self.file.stat().st_size
        self.resolve_source()
        self.parse()
    
    def resolve_source(self):
        try:
            self.source = Source.objects.get(name=self.filename)
            if self.source.md5 != self.md5 or self.source.size < self.filesize or Article.objects.filter(source_file=self.source).count() == 0 or self.force == True:
                self.source.delete()
                raise Source.DoesNotExist
            else:
                raise ValueError('File already parsed!')
        except Source.DoesNotExist:
            self.source = Source(name=self.filename, md5=self.md5, 
                                 size=self.filesize, path=self.path)
            self.source.save()

    def calc_md5(self):
        with open(self.path, 'rb') as f:
            hashobj = hashlib.md5(f.read())
            self.md5 = hashobj.hexdigest()
            logger.info(self.md5)
    
    def parse(self):
        try:
            self.read_file()
            self.parse_to_dict()
            self.pic_info()
            self.filt_dup()
            self.save_todb()
            self.update_none_field()
        except Exception as e:
            sys.exit(e)
    
    @timeit
    def read_file(self):
        if self.path.endswith('gz'):
            with gzip.open(self.path, 'rt') as f:
                self.xml_text = f.read()
        else:
            with open(self.path) as f:
                self.xml_text = f.read()
        logger.info('Read file done!')

    @timeit
    def parse_to_dict(self):
        self.xml_dict = xmltodict.parse(self.xml_text)
        logger.info('Parse to dict done!')
    
    @timeit
    def pic_info(self):
        pubarticles = self.xml_dict.get('PubmedArticleSet', {}).get('PubmedArticle', [])
        logger.info(len(pubarticles))
        # self.results = multiprocessing.Manager().list()
        self.results = []
        # pool = dummy.Pool(settings.PARSE_PROCESS)
        # pool.map_async(self.select_info_to_obj, pubarticles, error_callback=logger.info)
        # pool.close()
        # pool.join()
        for x in pubarticles:
            self.select_info_to_obj(x)
        logger.info(f'Raw results {len(self.results)}')
    
    @timeit
    def filt_dup(self):
        self.uniq_pmid = set()
        self.new_result = []
        self.dup = []
        for x in self.results:
            if x['pmid'] not in self.uniq_pmid:
                self.uniq_pmid.add(x['pmid'])
                self.new_result.append(x)
            else:
                self.dup.append(x)
        self.results = self.new_result
        logger.info(f'Uniq Results {len(self.results)}')
    
    @timeit
    def save_todb(self):
        db_objs = []
        for x in self.results:
            obj = Article(source_file = self.source, **x)            
            db_objs.append(obj)
        Article.objects.bulk_create(db_objs)        

    @timeit
    def update_none_field(self):
        count = 0
        for x in self.dup:
            if x['abstract']:
                try:
                    obj = Article.objects.get(pmid=x['pmid'], abstract__isnull=True)    
                    obj.abstract = x['abstract']
                    obj.save()
                    count += 1
                except Article.DoesNotExist:
                    pass
        logger.info(f'Updated {count} record using dup info')

    def select_info_to_obj(self, pubarticle):
        info = pubarticle.get('MedlineCitation', {})
        pmid = info.get('PMID', {}).get('#text')
        if pmid:
            pmid = int(pmid)
        else:
            pmid = 0
        journal = info.get('Article', {}).get('Journal', {}).get( 'Title')
        issue = info.get('Article', {}).get('Journal', {}).get('JournalIssue', {}).get('Issue')
        volume = info.get('Article', {}).get('Journal', {}).get('JournalIssue', {}).get('Volume')
        pubyear = info.get('Article', {}).get('Journal', {}).get('JournalIssue', {}).get('PubDate', {}).get('Year', 1000)
        month = info.get('Article', {}).get('Journal', {}).get('JournalIssue', {}).get('PubDate', {}).get('Month', 1)
        pubday = info.get('Article', {}).get('Journal', {}).get('JournalIssue', {}).get('PubDate', {}).get('Day', 1)
    
        pubmonth = self.month_map.get(month, 1)
        title = info.get('Article', {}).get('ArticleTitle')
        if type(title) == collections.OrderedDict:
            title = title.get('#text')
        page = info.get('Article', {}).get('Pagination', {}).get('MedlinePgn')
        abstract = info.get('Article', {}).get('Abstract', {}).get('AbstractText')
        if type(abstract) == list:
            temp_list = [x.get('#text', str(x)) for x in abstract if type(x) == collections.OrderedDict]
            abstract = ' '.join(temp_list)
        elif type(abstract) == collections.OrderedDict:
            abstract = abstract.get('#text')

        author = info.get('Article', {}).get('AuthorList', {}).get('Author', [{}])
        if type(author) == list:
            if len(author) > 0:
                author = author[0].get('Initials')
        elif type(author) == dict or type(author) == collections.OrderedDict:
            # logger.info(author)
            author = author.get('Initials')

        language = info.get('Article', {}).get('Language', '')
        if type(language) == list:
            language = ','.join(language)
        try:
            pubdate = datetime.date(year=int(pubyear), month=int(pubmonth), day=int(pubday))
        except Exception as e:
            logger.error(e)
            logger.info(pubyear, pubmonth, pubday)
            
        obj = dict(pmid=pmid,journal=journal,issue=issue, volume=volume, pubdate=pubdate, title=title, page=page, abstract=abstract, author=author, language=language)
        self.results.append(obj)
