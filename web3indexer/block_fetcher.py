from datetime import datetime

import structlog

from .task import FetchBlockTask, ProcessLogTask


logger = structlog.get_logger()


class BlockFetcher:
    """
    Handles fetching information for a given block and scheduling `ProcessLog` tasks.
    """

    MAX_RETRIES = 5

    def fetch_with_retry(self, dispatcher, w3, task):
        try:
            self.fetch(dispatcher, w3, task)
        except Exception as e:
            logger.error(e)
            if task.retries < self.MAX_RETRIES:
                dispatcher.put(FetchBlockTask(block_number=task.block_number))
            else:
                raise Exception(
                    "Reached max number of retries for fetching block number {}".format(
                        task.block_number
                    )
                )

    def fetch(self, dispatcher, w3, task):
        logger.info("Fetching block", block_number=task.block_number)

        block = w3.eth.get_block(task.block_number)
        timestamp = datetime.fromtimestamp(block.timestamp)

        for transaction in block.transactions:
            txn_receipt = w3.eth.get_transaction_receipt(transaction)

            for log_index, log in enumerate(txn_receipt.logs):
                dispatcher.put(
                    ProcessLogTask(
                        block_number=task.block_number,
                        log=log,
                        log_index=log_index,
                        timestamp=timestamp,
                    )
                )
