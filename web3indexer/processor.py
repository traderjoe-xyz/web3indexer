import json
from enum import Enum

import structlog
from web3 import Web3

from .crud import upsert_transfer
from .models import Transfer
from .task import Task, ScrapeTask
from .utils import read_file


log = structlog.get_logger()


class ContractType(Enum):
    ERC721 = "ERC721"
    ERC1155 = "ERC721"


ERC165_ABI = read_file("abi/ERC165.json")
ERC721_ABI = read_file("abi/ERC721.json")
ERC721_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ERC_165_IDENTIFIER = "01ffc9a7"
ERC_721_IDENTIFIER = "80ac58cd"


class BlockProcessor:
    """
    Custom class for fetching, parsing and processing a block from the blockchain.
    """

    def __init__(self, db):
        self.db = db

    def process(self, dispatcher, w3, task):
        block = w3.eth.get_block(task.block_number)

        for transaction in block.transactions:
            txn_receipt = w3.eth.get_transaction_receipt(transaction)

            for log in txn_receipt.logs:
                topics = log.topics
                if len(topics) == 4 and topics[0].hex() == ERC721_TRANSFER_TOPIC:
                    if self._supports_erc721(w3, log.address):
                        print("LOG:", log)
                        (transfer_from, transfer_to, tokenId, transaction_hash) = self._parse_erc721_transfer_log(log)
                        try:
                            upsert_transfer(
                                self.db,
                                Transfer(
                                    contract=log.address,
                                    token_id=tokenId,
                                    transaction_hash=transaction_hash,
                                    transfer_from=transfer_from,
                                    transfer_to=transfer_to,
                                ),
                            )
                        except Exception as e:
                            print(e)

    def _parse_erc721_transfer_log(self, log):
        topics = log.topics
        transfer_from = "0x{}".format(topics[1].hex()[26:])
        transfer_to = "0x{}".format(topics[2].hex()[26:])
        token_id = int(topics[3].hex(), 16)
        transaction_hash = log.transactionHash.hex()
        return (transfer_from, transfer_to, token_id, transaction_hash)

    def _supports_erc721(self, w3, contract_address):
        try:
            erc165_contract = w3.eth.contract(address=contract_address, abi=ERC165_ABI)
            return erc165_contract.functions.supportsInterface(ERC_721_IDENTIFIER).call()
        except Exception as e:
            print("EXCEPTION:", e)
            return False
