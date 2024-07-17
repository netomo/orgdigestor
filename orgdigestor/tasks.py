# tasks.py
import csv
import os
from dataclasses import dataclass, field, asdict
from celery import shared_task, group
from django.utils.text import slugify

from orgdigestor.models import Organization, Country, Industry
from orgdigestor.serializers import OrganizationSerializer


@dataclass
class OrganizationDigestReport:
    created: int = 0
    updated: int = 0
    errors: int = 0
    error_messages: list[str] = field(default_factory=list)


def split_csv_file(file_path, rows_per_file):
    """
    Split a CSV file into multiple files with a maximum number of rows.
    """
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        headers = reader.fieldnames

        batch = []
        batch_number = 0
        batch_files = []

        for i, row in enumerate(reader):
            if i % rows_per_file == 0:
                if batch:
                    batch_files.append(write_batch_to_file(batch, headers, batch_number, file_path))
                    batch = []
                    batch_number += 1
            batch.append(row)

        if batch:
            batch_files.append(write_batch_to_file(batch, headers, batch_number, file_path))

        return batch_files


def write_batch_to_file(batch, headers, batch_number, file_path):
    """
    Write a batch of rows to a new CSV file.
    """
    file_name, file_extension = os.path.splitext(file_path)
    new_file_path = f'{file_name}_batch_{batch_number}{file_extension}'

    with open(new_file_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(batch)

    return new_file_path


def map_org_row(row_dict):
    return {
        'id': row_dict.get('Organization Id'),
        'name': row_dict.get('Name'),
        'country': row_dict.get('Country'),
        'industry': row_dict.get('Industry'),
        'website': row_dict.get('Website'),
        'description': row_dict.get('Description'),
        'founded': row_dict.get('Founded'),
        'number_of_employees': row_dict.get('Number of employees'),
    }


@shared_task
def process_organizations_csv(file_path, rows_per_task):
    """
    Start point task to process a CSV file with organizations' data.
    The process is:
    - Split the file into multiple files with a maximum number of rows.
    - Process each file in a separate task.
    - Collect the reports from each task.
    - Send a summary report with the results (number of organizations created, updated, etc).
    """
    batch_files = split_csv_file(file_path, rows_per_task)
    chunks_group = group(process_csv_chunk.s(file_path) for file_path in batch_files)
    g = chunks_group | sum_reports.s(send_email=True)
    return g.delay().get()


@shared_task
def process_csv_chunk(file_path):
    """
    Process a CSV file with organizations data.
    """

    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        rows_group = group(create_update_organization.s(data=map_org_row(row)) for row in reader)
        g = rows_group | sum_reports.s()
        digest_report = g.delay().get()

    os.remove(file_path)
    return digest_report


@shared_task(bind=True, retry_limit=2, default_retry_delay=5)
def create_update_organization(self, data):
    """
    Create or update an organization based on the data provided.
    """

    digest_report = OrganizationDigestReport()
    try:
        # Create or update the organization
        organization_id = data.pop('id')

        country_name = data.get('country')
        country, _ = Country.objects.get_or_create(name=country_name)
        data['country'] = country.id

        industry_slug = slugify(data.get('industry'))
        try:
            industry = Industry.objects.get(slug=industry_slug)
        except Industry.DoesNotExist:
            industry = Industry.objects.create(name=data.get('industry'), slug=industry_slug)
        data['industry'] = industry.id

        serializer = OrganizationSerializer(data=data)
        serializer.is_valid(raise_exception=True)

        organization, created = Organization.objects.update_or_create(
            id=organization_id,
            defaults=serializer.validated_data
        )

        if created:
            digest_report.created = 1
        else:
            digest_report.updated = 1

    except Exception as e:
        if self.request.retries < self.max_retries:
            raise self.retry(exc=e)
        else:
            digest_report.errors = 1
            digest_report.error_messages.append(str(e))

    return asdict(digest_report)


@shared_task
def sum_reports(reports, send_email=False):
    """
    Process the reports from each chunk and generate a summary report.
    """
    summary_report = OrganizationDigestReport()
    for report in reports:
        summary_report.created += report['created']
        summary_report.updated += report['updated']
        summary_report.errors += report['errors']
        summary_report.error_messages.extend(report['error_messages'])

    if send_email:
        print('Sending email...')

    print('=== Summary report ===')
    print(f'Created: {summary_report.created}')
    print(f'Updated: {summary_report.updated}')
    print(f'Errors: {summary_report.errors}')
    print('Error messages:')

    return asdict(summary_report)
