import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0007_step_output'),
    ]

    operations = [
        migrations.CreateModel(
            name='SourceFile',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('file_name', models.CharField(max_length=512)),
                ('file_hash', models.CharField(max_length=64)),
                ('processed_at', models.DateTimeField(auto_now_add=True)),
                (
                    'tenant',
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to='src.tenant',
                    ),
                ),
                (
                    'job',
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to='src.job',
                    ),
                ),
            ],
            options={
                'db_table': 'source_files',
            },
        ),
        migrations.AlterUniqueTogether(
            name='sourcefile',
            unique_together={('tenant', 'file_hash')},
        ),
    ]
