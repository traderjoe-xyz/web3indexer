from collections import namedtuple


Task = namedtuple("Task", "collector address last_block")
ScheduledTask = namedtuple("ScheduledTask", "task not_before")
ScrapeTask = namedtuple(
    "GenericEventCollector", "collector abi address event from_block retries"
)
FetchBlockTask = namedtuple("FetchBlockTask", "block_number")
ProcessLogTask = namedtuple(
    "ProcessLogTask", "block_number log log_index timestamp"
)
