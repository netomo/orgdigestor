from rest_framework import serializers

from orgdigestor.models import Organization


class OrganizationSerializer(serializers.ModelSerializer):

    class Meta:
        model = Organization
        fields = ('id', 'name', 'country', 'industry', 'website', 'description', 'founded', 'number_of_employees')

    def to_internal_value(self, data):
        founded_str = data.get('founded')
        if founded_str and len(founded_str) == 4:
            data['founded'] = founded_str + '-01-01'
        return super().to_internal_value(data)


class OrganizationsFileDigestSerializer(serializers.Serializer):
    file = serializers.FileField(
        help_text='File to digest, a CSV file with organizations data. Zip files are also accepted.',
        allow_empty_file=False,
        allow_null=False,
        required=True,
        use_url=False,
    )
    rows_per_task = serializers.IntegerField(
        help_text='Number of rows to digest from the file. If not provided, all rows will be digested.',
        default=10000,
    )

