# tasks.py
import csv
import os

from celery import shared_task
from .models import Organization, Country, Industry


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
    batch_files = split_csv_file(file_path, rows_per_task)
    print(batch_files)
