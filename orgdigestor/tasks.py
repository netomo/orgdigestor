# tasks.py
import csv
import os
from dataclasses import dataclass, field, asdict
from celery import shared_task, group
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


@shared_task
def process_organizations_csv(file_path, rows_per_task):
    """
    Main orchestration task to process a CSV file with organizations data.
    The process is:
    - Split the file into multiple files with a maximum number of rows.
    - Process each file in a separate task.
    - Collect the reports from each task.
    - Send a summary report with the results (number of organizations created, updated, etc).
    """
    batch_files = split_csv_file(file_path, rows_per_task)
    digest_group = group(process_organizations_csv_chunk.s(file_path) for file_path in batch_files)
    chain = digest_group | process_organizations_csv_summary.s()
    chain()



@shared_task
def process_organizations_csv_chunk(file_path):
    """
    Process a CSV file with organizations data. Each row is a dictionary with the following keys:
    - Organization Id
    - Name
    - Website
    - Country
    - Description
    - Founded
    - Industry
    - Number of employees
    """
    digest_report = OrganizationDigestReport()

    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        countries = {}
        industries = {}

        for row in reader:
            organization_id = row.get('Organization Id')
            name = row.get('Name')
            website = row.get('Website')
            country_name = row.get('Country')
            description = row.get('Description')
            founded = row.get('Founded')
            industry_name = row.get('Industry')
            number_of_employees = row.get('Number of employees')

            try:
                # Create or update the organization
                if country_name not in countries:
                    country, _ = Country.objects.get_or_create(name=country_name)
                    countries[country_name] = country
                else:
                    country = countries[country_name]

                if industry_name not in industries:
                    industry, _ = Industry.objects.get_or_create(name=industry_name)
                    industries[industry_name] = industry
                else:
                    industry = industries[industry_name]

                serializer = OrganizationSerializer(data={
                    'id': organization_id,
                    'name': name,
                    'website': website,
                    'country': country.pk,
                    'industry': industry.pk,
                    'description': description,
                    'founded': founded,
                    'number_of_employees': number_of_employees,
                })
                serializer.is_valid(raise_exception=True)

                organization, created = Organization.objects.update_or_create(
                    id=organization_id,
                    defaults=serializer.validated_data
                )

                if created:
                    digest_report.created += 1
                else:
                    digest_report.updated += 1
            except Exception as e:
                digest_report.errors += 1
                digest_report.error_messages.append(str(e))

    # delete the file after processing
    os.remove(file_path)
    return asdict(digest_report)


@shared_task
def process_organizations_csv_summary(reports):
    """
    Process the reports from each chunk and generate a summary report.
    """
    summary_report = OrganizationDigestReport()
    for report in reports:
        summary_report.created += report['created']
        summary_report.updated += report['updated']
        summary_report.errors += report['errors']
        summary_report.error_messages.extend(report['error_messages'])

    print('=== Summary report ===')
    print(f'Created: {summary_report.created}')
    print(f'Updated: {summary_report.updated}')
    print(f'Errors: {summary_report.errors}')
    print('Error messages:')

    return summary_report
