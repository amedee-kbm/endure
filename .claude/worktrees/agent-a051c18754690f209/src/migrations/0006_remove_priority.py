from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0005_job_periodic_task'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='job',
            name='priority',
        ),
        migrations.RemoveField(
            model_name='periodictask',
            name='priority',
        ),
    ]
