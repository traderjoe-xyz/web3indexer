from collections import namedtuple


Task = namedtuple("Task", "collector address last_block")
ScheduledTask = namedtuple("ScheduledTask", "task not_before")
