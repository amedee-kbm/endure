import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0004_remove_tenant_priority_boost'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='periodic_task',
            field=models.ForeignKey(
                blank=True,
                db_column='periodic_task_id',
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name='spawned_jobs',
                to='src.periodictask',
            ),
        ),
    ]
