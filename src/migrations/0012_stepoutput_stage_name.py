from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0011_remove_stepoutput_error_alter_job_state_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='stepoutput',
            name='stage_name',
            field=models.TextField(default=''),
        ),
        migrations.AlterUniqueTogether(
            name='stepoutput',
            unique_together={('job', 'stage_name', 'step_id')},
        ),
    ]
