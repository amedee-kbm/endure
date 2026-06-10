from django.contrib import admin

from src.models import (
    Checkpoint,
    DeadLetterJob,
    Job,
    JobEvent,
    PeriodicTask,
    SchedulerLeader,
    SourceFile,
    StepOutput,
    Tenant,
    Worker,
)

admin.site.register(Job)
admin.site.register(Tenant)
admin.site.register(Worker)
admin.site.register(Checkpoint)
admin.site.register(StepOutput)
admin.site.register(JobEvent)
admin.site.register(PeriodicTask)
admin.site.register(DeadLetterJob)
admin.site.register(SchedulerLeader)
admin.site.register(SourceFile)
