# Generated by Django 2.0.8 on 2018-10-08 08:02

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Article',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('pmid', models.IntegerField(unique=True)),
                ('journal', models.CharField(max_length=100)),
                ('pubdate', models.DateField()),
                ('volume', models.CharField(max_length=20, null=True)),
                ('issue', models.CharField(max_length=20, null=True)),
                ('title', models.TextField()),
                ('abstract', models.TextField(null=True)),
                ('page', models.CharField(max_length=20, null=True)),
                ('author', models.CharField(max_length=10, null=True)),
                ('language', models.CharField(max_length=15, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Source',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=50, unique=True)),
                ('md5', models.CharField(max_length=32, unique=True)),
                ('size', models.BigIntegerField()),
                ('path', models.CharField(max_length=100)),
            ],
        ),
        migrations.AddField(
            model_name='article',
            name='source_file',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='core.Source'),
        ),
    ]