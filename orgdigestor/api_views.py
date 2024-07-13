from http import HTTPMethod
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.parsers import MultiPartParser, FormParser

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
        return self.list(request)
