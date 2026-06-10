from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('src', '0008_sourcefile'),
    ]

    operations = [
        migrations.AddField(
            model_name='checkpoint',
            name='data',
            field=models.BinaryField(default=b''),
        ),
        migrations.RemoveField(
            model_name='checkpoint',
            name='storage_path',
        ),
    ]
