from django.urls import path, include
from rest_framework import routers

from orgdigestor.api_views import OrganizationViewSet


router = routers.DefaultRouter()
router.register(r'organizations', OrganizationViewSet)

urlpatterns = [
    path('', include(router.urls)),
]
