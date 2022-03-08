import json

import structlog

from .task import Task, ScrapeTask


log = structlog.get_logger()


class Collector:
    """
    Custom classes for indexing the blockchain.
    """

    def collect(self, dispatcher, w3, task):
        """
        Collects the actual information from the blockchain.
        """
        raise NotImplementedError()


def _read_file(path):
    with open(path) as fh:
        return fh.read()


class UniSwapFactoryCollector:

    abi = _read_file("abi/JoeFactory.json")

    def collect(self, dispatcher, w3, task):
        factory = w3.eth.contract(
            abi=self.abi,
            address=task.address,
        )
        for i in range(factory.functions.allPairsLength().call()):
            pair_address = factory.functions.allPairs(i).call()
            dispatcher.put(
                Task(
                    "UniSwapPairDataCollector",
                    pair_address,
                    None,
                )
            )
            log.info(
                "collected",
                collector=self.__class__.__name__,
                address=pair_address,
            )


class UniSwapPairDataCollector:

    abi = _read_file("abi/JoePair.json")

    def collect(self, dispatcher, w3, task):
        pair = w3.eth.contract(
            abi=self.abi,
            address=task.address,
        )
        dispatcher.put(
            Task(
                "UniSwapPairSwapCollector",
                task.address,
                w3.eth.get_block_number(),
            )
        )
        log.info(
            "collected",
            collector=self.__class__.__name__,
            address=task.address,
        )


class UniSwapPairSwapCollector:

    abi = _read_file("abi/JoePair.json")

    def collect(self, dispatcher, w3, task):
        pair = w3.eth.contract(
            abi=self.abi,
            address=task.address,
        )
        to_block = w3.eth.get_block_number()
        events = pair.events.Swap.createFilter(
            fromBlock=task.last_block,
            toBlock=to_block,
        ).get_all_entries()
        for event in events:
            log.info(
                "collected",
                collector=self.__class__.__name__,
                swap=event,
            )
        dispatcher.schedule(
            Task(
                task.collector,
                task.address,
                to_block,
            ),
            60
        )


class GenericEventCollector:

    BLOCKS_CHUNK_SIZE = 10000

    def collect_with_retry(self, dispatcher, w3, task):
        try:
            self.collect(dispatcher, w3, task)
        except Exception as exc:
            log.error(exc)
            # TODO: retry mechanism
            raise

    def collect(self, dispatcher, w3, task):
        contract = w3.eth.contract(
            abi=task.abi,
            address=task.address,
        )

        # Get the to_block to check.
        cur_block = w3.eth.get_block_number()
        if (cur_block - task.from_block) < self.BLOCKS_CHUNK_SIZE:
            end_block = cur_block
        else:
            end_block = task.from_block + self.BLOCKS_CHUNK_SIZE

        log.info('in collect', address=task.address, from_block = task.from_block, to_block=end_block)
        events = getattr(contract.events, task.event).createFilter(
            fromBlock=task.from_block,
            toBlock=end_block,
        ).get_all_entries()

        for event in events:
            log.info(
                'collected',
                name=task.event,
                collector=self.__class__.__name__,
                event_struct=event,
            )

        dispatcher.put(
            ScrapeTask(
                abi=task.abi,
                address=task.address,
                event=task.event,
                from_block=end_block,
                collector=task.collector,
            )
        )
