import xmltodict
import gzip
import json
import sqlite3
import collections
import glob
import os


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
    def __init__(self, inf, dbfile=None, outf=None):
        self.obj_result = []
        self.iter_result = []
        self.dict_result = []
        self.xml_text = None
        self.xmldict = None

        self.infs = glob.glob(inf)
        self.outf = outf
        self.dbfile = dbfile

        self.total_count = 0

        # init db
        self.db_init()

        for x in self.infs:
            self.inf = x
            self.obj_result = []
            # extract info to dict and make article object 
            self.parse()
            # save to dbfile
            if self.dbfile is not None:
                self.parse_to_db()
        # close db
        self.conn.close()
    
    def parse(self):
        self.read_file()
        self.parse_to_dict()
        self.extract_info()
        
    def parse_to_db(self):
        self.extract_info_to_iter()
        self.to_db()
    
    def parse_to_json(self):
        self.extract_info_to_dict()
        self.to_json()
    
    def db_init(self):
        if os.path.exists(self.dbfile):
            os.remove(self.dbfile)
        self.conn = sqlite3.connect(self.dbfile)
        self.cursor = self.conn.cursor()
        try:
            self.cursor.execute('''CREATE TABLE pubmed 
            (pmid text, journal text, pubdate text, page text, volume text, issue text, title text, abstract text, author text, language text)
            '''
            )
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

    def read_file(self):
        with gzip.open(self.inf, 'rt') as f:
            self.xml_text = f.read()
        print('Read file done!')
    
    def parse_to_dict(self):
        self.xmldict = xmltodict.parse(self.xml_text)
        print('Parse to dict done!')
    
    def to_json(self):
        with open(self.outf, 'w') as f:
            json.dump(self.dict_result, f, indent=4)
    
    def to_db(self):
        # self.cursor.executemany("insert into pubmed values (?,?,?,?,?,?,?,?,?,?)", self.iter_result)
        for x in self.iter_result:
            self.total_count += 1
            try:
                self.cursor.execute("insert into pubmed values (?,?,?,?,?,?,?,?,?,?)", x)
            except Exception as e:
                print(e)
                print(x)
                raise(e)      
        self.conn.commit()
    
    def extract_info_to_dict(self):
        self.dict_result = [x.to_dict() for x in self.obj_result]

    def extract_info_to_iter(self):
        self.iter_result = [x.to_iter() for x in self.obj_result]
    
    def extract_info(self):
        article_set = self.xmldict.get('PubmedArticleSet', {}).get('PubmedArticle', {})
        print(f'Total {len(article_set)}')
        count = 0
        for article in article_set:
            count += 1
            if count % 1000 == 0:
                print(f'Processing {count}')
            self.select_info_to_obj(article)
        

    def select_info_to_obj(self, pubarticle):
        info = pubarticle.get('MedlineCitation', {})
        pmid = info.get('PMID', {}).get('#text')
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
            temp_list = [x.get('#text') for x in abstract if type(x) == collections.OrderedDict]
            abstract = ' '.join(temp_list)
        elif type(abstract) == collections.OrderedDict:
            abstract = abstract.get('#text')

        author = info.get('Article', {}).get('AuthorList', {}).get('Author', [{}])
        if type(author) == list:
            if len(author) > 0:
                author = author[0].get('Initials')
        elif type(author) == dict or type(author) == collections.OrderedDict:
            # print(author)
            author = author.get('Initials')

        language = info.get('Article', {}).get('Language')
        if type(language) == list:
            language = ','.join(language)
        article = Article(pmid=pmid,journal=journal,issue=issue, volume=volume, pubyear=pubyear, 
                          pubmonth=pubmonth, title=title, page=page, abstract=abstract, author=author, language=language)
        self.obj_result.append(article)

    

if __name__ == '__main__':
    Pmparse(inf='/Users/seahurt/Public/PycharmProjects/pubmed/*.xml.gz', dbfile='test.db', outf='test.json')
