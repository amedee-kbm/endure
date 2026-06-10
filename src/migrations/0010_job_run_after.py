from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0009_checkpoint_data'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='run_after',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
