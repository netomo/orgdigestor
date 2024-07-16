from django.db import models


class Country(models.Model):
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name


class Industry(models.Model):
    name = models.CharField(max_length=100)
    slug = models.SlugField(max_length=100)

    def __str__(self):
        return self.name


class Organization(models.Model):

    id = models.CharField(primary_key=True, max_length=15, editable=False)
    name = models.CharField(max_length=100)
    country = models.ForeignKey(Country, on_delete=models.CASCADE)
    industry = models.ForeignKey(Industry, on_delete=models.CASCADE)

    website = models.URLField(blank=True)
    description = models.TextField(blank=True)
    founded = models.DateField(null=True)
    number_of_employees = models.IntegerField(null=True)

    def __str__(self):
        return self.name
