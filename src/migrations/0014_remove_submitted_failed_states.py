from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0013_alter_stepoutput_options_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='job',
            name='state',
            field=models.CharField(
                choices=[
                    ('QUEUED', 'Queued'),
                    ('SCHEDULED', 'Scheduled'),
                    ('RUNNING', 'Running'),
                    ('COMPLETED', 'Completed'),
                    ('TIMED_OUT', 'Timed Out'),
                    ('CANCELLED', 'Cancelled'),
                    ('DEAD_LETTER', 'Dead Letter'),
                ],
                db_index=True,
                default='QUEUED',
                max_length=16,
            ),
        ),
        migrations.AlterModelTable(
            name='stepoutput',
            table='step_outputs',
        ),
    ]
