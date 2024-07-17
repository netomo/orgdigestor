import os
import csv
import uuid
from http import HTTPMethod
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.pagination import CursorPagination
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response

from orgdigestor.models import Organization
from orgdigestor.serializers import OrganizationSerializer, OrganizationsFileDigestSerializer
from orgdigestor.tasks import process_organizations_csv


class OrganizationPagination(CursorPagination):
    page_size = 10
    ordering = 'id'


class OrganizationViewSet(viewsets.ModelViewSet):
    queryset = Organization.objects.all()
    serializer_class = OrganizationSerializer
    parser_classes = (MultiPartParser, FormParser)
    pagination_class = OrganizationPagination

    @action(
        detail=False,
        methods=[HTTPMethod.POST],
        serializer_class=OrganizationsFileDigestSerializer,
        url_path='digest'
    )
    def digest(self, request):
        serializer = OrganizationsFileDigestSerializer(data=request.data)
        if serializer.is_valid():
            file = serializer.validated_data['file']

            validation_error = self.validate_csv_file(file)
            if validation_error:
                return Response({'error': validation_error}, status=status.HTTP_400_BAD_REQUEST)

            file_path = self.save_file(file)
            rows_per_task = serializer.validated_data.get('rows_per_task', 10000)
            process_organizations_csv.delay(file_path, rows_per_task)
            return Response(
                {'status': 'Aww yeah, file is valid and being processed!'},
                status=status.HTTP_202_ACCEPTED
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @staticmethod
    def validate_csv_file(file):
        """
        Immediate validation of the uploaded file.
        """
        required_headers = {'Organization Id', 'Name', 'Country', 'Industry'}
        try:
            if not file.name.lower().endswith('.csv'):
                return "The uploaded file must be a CSV file."

            file.seek(0)
            reader = csv.DictReader(file.read().decode('utf-8').splitlines())
            headers = set(reader.fieldnames)

            if not required_headers.issubset(headers):
                return f"Missing required headers: {required_headers - headers}"
            return
        except Exception as e:
            return str(e)

    @staticmethod
    def save_file(file):
        """
        We would be using something like S3 normally, but for the sake of this test we will save the file
        in the "local" filesystem that is shared between the Django app and the Celery worker.
        """
        file_dir = '/mnt/data/'
        os.makedirs(file_dir, exist_ok=True)

        # Create a unique file name
        unique_id = uuid.uuid4()
        unique_file_name = f"{unique_id}_{file.name}"
        file_path = os.path.join(file_dir, unique_file_name)

        # Save the file
        with open(file_path, 'wb+') as destination:
            for chunk in file.chunks():
                destination.write(chunk)

        return file_path
