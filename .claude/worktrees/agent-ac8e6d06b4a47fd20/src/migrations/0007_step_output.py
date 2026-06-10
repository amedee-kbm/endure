import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0006_remove_priority'),
    ]

    operations = [
        migrations.CreateModel(
            name='StepOutput',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('step_id', models.IntegerField()),
                ('step_name', models.TextField()),
                ('output', models.TextField()),
                ('error', models.TextField(blank=True, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                (
                    'job',
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name='step_outputs',
                        to='src.job',
                    ),
                ),
            ],
            options={
                'ordering': ['step_id'],
            },
        ),
        migrations.AlterUniqueTogether(
            name='stepoutput',
            unique_together={('job', 'step_id')},
        ),
    ]
