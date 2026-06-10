from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("src", "0003_job_result"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="tenant",
            name="priority_boost",
        ),
    ]
