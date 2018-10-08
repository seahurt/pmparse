from django.db import models

# Create your models here.
class Article(models.Model):
    pmid = models.IntegerField(unique=True)
    journal = models.CharField(max_length=100)
    pubdate = models.DateField()
    volume = models.CharField(max_length=20, null=True)
    issue = models.CharField(max_length=20, null=True)
    title = models.TextField()
    abstract = models.TextField(null=True)
    page = models.CharField(max_length=20, null=True)
    author = models.CharField(max_length=10, null=True)
    language = models.CharField(max_length=15, null=True)
    source_file = models.ForeignKey('Source', on_delete=models.CASCADE)

    def __str__(self):
        return str(self.pmid)


class Source(models.Model):
    name = models.CharField(max_length=50, unique=True)
    md5 = models.CharField(max_length=32, unique=True)
    size = models.BigIntegerField()
    path = models.CharField(max_length=100)

    def __str__(self):
        return str(self.name) + '_' + str(self.article_set.count())
