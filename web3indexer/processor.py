import json
from enum import Enum

import structlog
from web3 import Web3

from .crud import get_contract, upsert_contract, upsert_transfer
from .models import Contract, Transfer
from .task import Task, ScrapeTask
from .utils import read_file


logger = structlog.get_logger()


class ContractType(Enum):
    ERC721 = "ERC721"
    ERC1155 = "ERC721"


ERC165_ABI = read_file("abi/ERC165.json")
ERC721_ABI = read_file("abi/ERC721.json")
ERC721_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
ERC_165_IDENTIFIER = "01ffc9a7"
ERC_721_IDENTIFIER = "80ac58cd"
ERC_721_METADATA_IDENTIFIER = "5b5e139f"
ERC_1155_IDENTIFIER = "d9b67a26"


class BlockProcessor:
    """
    Custom class for fetching, parsing and processing a block from the blockchain.
    """

    def __init__(self, db):
        self.db = db

    def process(self, dispatcher, w3, task):
        logger.info("Processing block", block_number=task.block_number)

        block = w3.eth.get_block(task.block_number)

        for transaction in block.transactions:
            txn_receipt = w3.eth.get_transaction_receipt(transaction)

            for log in txn_receipt.logs:
                topics = log.topics
                contract_address = log.address
                if (
                    len(topics) == 4
                    and topics[0].hex() == ERC721_TRANSFER_TOPIC
                ):
                    if self._supports_erc721(w3, contract_address):
                        (
                            transfer_from,
                            transfer_to,
                            tokenId,
                            transaction_hash,
                        ) = self._parse_erc721_transfer_log(log)
                        try:
                            logger.info(
                                "Processing ERC721 transfer",
                                transfer_from=transfer_from,
                                transfer_to=transfer_to,
                            )
                            upsert_transfer(
                                self.db,
                                Transfer(
                                    contract=contract_address,
                                    token_id=tokenId,
                                    transaction_hash=transaction_hash,
                                    transfer_from=transfer_from,
                                    transfer_to=transfer_to,
                                ),
                            )
                            self._upsert_contract(w3, contract_address)
                        except Exception as e:
                            print(e)

    def _upsert_contract(self, w3, contract_address):
        contract = get_contract(self.db, contract_address)
        if contract is None:
            (name, symbol) = self._fetch_erc721_metadata(w3, contract_address)
            logger.info("Upserting new contract", name=name, symbol=symbol)
            upsert_contract(
                self.db,
                Contract(
                    address=contract_address,
                    name=name,
                    symbol=symbol,
                    contract_type=ContractType.ERC721.value,
                ),
            )

    def _fetch_erc721_metadata(self, w3, contract_address):
        try:
            supports_erc721_metadata = self._supports_erc721_metadata(
                w3, contract_address
            )
            if supports_erc721_metadata:
                erc721_contract = w3.eth.contract(
                    address=contract_address, abi=ERC721_ABI
                )
                return (
                    erc721_contract.functions.name().call(),
                    erc721_contract.functions.symbol().call(),
                )
            return (None, None)
        except Exception as e:
            return (None, None)

    def _parse_erc721_transfer_log(self, log):
        topics = log.topics
        transfer_from = "0x{}".format(topics[1].hex()[26:])
        transfer_to = "0x{}".format(topics[2].hex()[26:])
        token_id = int(topics[3].hex(), 16)
        transaction_hash = log.transactionHash.hex()
        return (transfer_from, transfer_to, token_id, transaction_hash)

    def _supports_erc721(self, w3, contract_address):
        try:
            erc165_contract = w3.eth.contract(
                address=contract_address, abi=ERC165_ABI
            )
            return erc165_contract.functions.supportsInterface(
                ERC_721_IDENTIFIER
            ).call()
        except Exception as e:
            return False

    def _supports_erc721_metadata(self, w3, contract_address):
        try:
            erc165_contract = w3.eth.contract(
                address=contract_address, abi=ERC165_ABI
            )
            return erc165_contract.functions.supportsInterface(
                ERC_721_METADATA_IDENTIFIER
            ).call()
        except Exception as e:
            return False
