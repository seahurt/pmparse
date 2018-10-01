import xmltodict
import gzip
import json
import sqlite3
import collections
import glob
import os
import sys
from multiprocessing import dummy, Pool, Manager
import logging

logger = logging.getLogger('main')
logger.setLevel(logging.INFO)  # 必须有

FORMAT = '%(asctime)s  %(levelname)s %(message)s'
fmt = logging.Formatter(FORMAT)

h = logging.FileHandler('run.log')
h.setFormatter(fmt)

sth = logging.StreamHandler()
sth.setFormatter(fmt)

logger.addHandler(h)
logger.addHandler(sth)




class Article(object):
    def __init__(self, pmid=None, journal=None, pubyear=None, pubmonth=None, language=None,
                 page=None, volume=None, issue=None, title=None, abstract=None, author=None):
        if pmid is None:
            raise ValueError('PMID cannot be null')
        self.pmid = pmid
        self.journal = journal
        self.pubyear = pubyear
        self.pubmonth = pubmonth
        self.page = page
        self.volume = volume
        self.issue = issue
        self.title = title
        self.abstract = abstract
        self.author = author
        self.language = language
        if self.pubyear and self.pubmonth:
            self.pubdate = self.pubyear + '-' + self.pubmonth.zfill(2)
        elif self.pubyear:
            self.pubdate = self.pubyear
        else:
            self.pubdate = None
        
    
    def to_dict(self):
        return {
            'PMID': self.pmid,
            'Journal': self.journal,
            # 'PubYear': self.pubyear,
            # 'PubMonth': self.pubmonth,
            'PubDate': self.pubdate,
            'Page': self.page,
            'Volume': self.volume,
            'Issue': self.issue,
            'Title': self.title,
            'Abstract': self.abstract,
            'Author': self.author,
            'Language': self.language
        }
    
    def to_iter(self):
        res = (self.pmid, self.journal, self.pubdate, self.page, self.volume, self.issue, self.title, self.abstract, self.author, self.language)
        #return tuple(map(str, res))
        return res


class Pmparse(object):
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
    def __init__(self, indir, dbfile=None, process=10, test=False):
        self.process = int(process)

        self.infs = glob.glob(indir + '/*.xml.gz')

        if test:
            logger.info('Start test mode')
            self.infs = self.infs[:30]

        self.dbfile = dbfile

        logger.info(f'Total file {len(self.infs)}')

        self.total_count = Manager().list()

        

        p = Pool(self.process)
        logger.info(f'Start pool {self.process}')
        p.map_async(self.parse_to_db, self.infs, callback=print, error_callback=print)
        p.close()
        p.join()
        

        # init db
        logger.info('init db')
        self.db_init()

        self.total_to_db()
        # close db
        self.conn.close()
        logger.info(f'Total count {len(self.total_count)}')  
        
    def parse_to_db(self, f):
        logger.info(f'Start parse {f}')
        try:
            xml_text = self.read_file(f)
            xml_dict = self.parse_to_dict(xml_text)
            iter_result = self.extract_info_to_iter(xml_dict)
            #self.to_db(iter_result)
            self.total_count += iter_result
        except Exception as e:
            logger.error(f'{f} error')
    
    def db_init(self):
        if os.path.exists(self.dbfile):
            os.remove(self.dbfile)
        self.conn = sqlite3.connect(self.dbfile)
        self.cursor = self.conn.cursor()
        try:
            self.cursor.execute('''CREATE TABLE pubmed 
            (pmid INTEGER, journal text, pubdate text, page text, volume text, issue text, title text, abstract text, author text, language text)
            '''
            )
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

    def read_file(self, file):
        with gzip.open(file, 'rt') as f:
            xml_text = f.read()
        logger.info('Read file done!')
        return xml_text
    
    def parse_to_dict(self, xml_text):
        xml_dict = xmltodict.parse(xml_text)
        logger.info('Parse to dict done!')
        return xml_dict
    
    def to_db(self, iter_result):
        # self.cursor.executemany("insert into pubmed values (?,?,?,?,?,?,?,?,?,?)", self.iter_result)
        for x in iter_result:
            # self.total_count.append(0)
            try:
                self.cursor.execute("insert into pubmed values (?,?,?,?,?,?,?,?,?,?)", x)
            except Exception as e:
                logger.info(e)
                logger.info(x)
                raise(e)      
        self.conn.commit()
    
    def total_to_db(self):
        total = len(self.total_count)
        batch_size = 100000
        x = 0
        while x <= total:
            logger.info(f'Save {x} -- {x +batch_size}')
            try:
                self.cursor.executemany("insert into pubmed values (?,?,?,?,?,?,?,?,?,?)", self.total_count[x: x+batch_size])
            except Exception as e:
                logger.info(e)
                logger.info(x)    
            self.conn.commit()
            x += batch_size
    
    def extract_info_to_dict(self, xml_dict):
        dict_result = [x.to_dict() for x in self.extract_info(xml_dict)]
        return dict_result

    def extract_info_to_iter(self, xml_dict):
        iter_result = [x.to_iter() for x in self.extract_info(xml_dict)]
        return iter_result
    
    def extract_info(self, xml_dict):
        container = []
        article_set = xml_dict.get('PubmedArticleSet', {}).get('PubmedArticle', {})
        logger.info(f'Total {len(article_set)}')
        count = 0
        for article in article_set:
            count += 1
            if count % 1000 == 0:
                logger.info(f'Processing {count}')
            container.append(self.select_info_to_obj(article))
        return container
        

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
        pubyear = info.get('Article', {}).get('Journal', {}).get('JournalIssue', {}).get('PubDate', {}).get('Year')
        month = info.get('Article', {}).get('Journal', {}).get('JournalIssue', {}).get('PubDate', {}).get('Month')
        pubmonth = self.month_map.get(month)
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
        article = Article(pmid=pmid,journal=journal,issue=issue, volume=volume, pubyear=pubyear, 
                          pubmonth=pubmonth, title=title, page=page, abstract=abstract, author=author, language=language)
        return article

    

if __name__ == '__main__':
    try:
        test = sys.argv[4]
    except IndexError as e:
        test = False
    test = True if str(test).lower() == 'true' else False
    try:
        process = sys.argv[3]
    except IndexError as e:
        process = 10
    process = int(process)
    Pmparse(sys.argv[1], dbfile=sys.argv[2], process=process, test=test)
