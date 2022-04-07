import json

import structlog
from web3 import Web3

from .crud import insert_event
from .task import Task, ScrapeTask
from .utils import read_file


log = structlog.get_logger()


ERC165_ABI = read_file("abi/ERC165.json")
ERC721_ABI = read_file("abi/ERC721.json")
ERC721_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ERC_165_IDENTIFIER = "01ffc9a7";
ERC_721_IDENTIFIER = "80ac58cd";

class BlockProcessor:
    """
    Custom class for fetching, parsing and processing a block from the blockchain.
    """

    def process(self, dispatcher, w3, task):
        block = w3.eth.get_block(task.block_number)
        transactions = block.transactions

        for transaction in block.transactions:
            txn_receipt = w3.eth.get_transaction_receipt(transaction)

            for log in txn_receipt.logs:
                topics = log.topics
                if len(topics) > 0 and topics[0].hex() == ERC721_TRANSFER_TOPIC:
                    try:
                        erc165_contract = w3.eth.contract(address=log.address, abi=ERC165_ABI)
                        supports_erc721 = erc165_contract.functions.supportsInterface(ERC_721_IDENTIFIER).call()
                        print("FOUND ERC721 CONTRACT:", log.address)
                    except Exception as e:
                        pass