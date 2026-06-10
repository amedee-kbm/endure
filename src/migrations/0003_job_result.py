from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0002_periodictask'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='result',
            field=models.JSONField(blank=True, default=None, null=True),
        ),
    ]
