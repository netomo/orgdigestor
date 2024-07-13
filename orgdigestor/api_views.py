import csv
from http import HTTPMethod
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response

from orgdigestor.models import Organization
from orgdigestor.serializers import OrganizationSerializer, OrganizationsFileDigestSerializer


class OrganizationViewSet(viewsets.ModelViewSet):
    queryset = Organization.objects.all()
    serializer_class = OrganizationSerializer
    parser_classes = (MultiPartParser, FormParser)

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

            # TODO: celery task to digest the file
            return Response(
                {'status': 'Aww yeah, file is valid and being processed!'},
                status=status.HTTP_202_ACCEPTED
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def validate_csv_file(self, file):
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
