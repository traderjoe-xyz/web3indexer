import json
from enum import Enum

import structlog
from web3 import Web3

from .crud import (
    get_contract,
    get_nft,
    upsert_contract,
    upsert_nft,
    upsert_ownership,
    upsert_transfer,
)
from .models import Contract, Ownership, Nft, Transfer
from .task import Task, ScrapeTask
from .utils import get_nft_id, read_file


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
                    and self._supports_erc721(w3, contract_address)
                ):
                    (
                        transfer_from,
                        transfer_to,
                        token_id,
                        transaction_hash,
                    ) = self._parse_erc721_transfer_log(log)
                    logger.info(
                        "Processing ERC721 transfer",
                        transfer_from=transfer_from,
                        transfer_to=transfer_to,
                    )
                    supports_erc721_metadata = self._supports_erc721_metadata(
                        w3, contract_address
                    )
                    self._upsert_contract(
                        w3, contract_address, supports_erc721_metadata
                    )
                    self._upsert_nft(
                        w3,
                        contract_address,
                        token_id,
                        supports_erc721_metadata,
                    )
                    upsert_transfer(
                        self.db,
                        Transfer(
                            nft_id=get_nft_id(contract_address, token_id),
                            transaction_hash=transaction_hash,
                            transfer_from=transfer_from,
                            transfer_to=transfer_to,
                        ),
                    )
                    self._upsert_ownerships(
                        contract_address,
                        token_id,
                        transfer_from,
                        transfer_to,
                    )

    def _upsert_contract(
        self, w3: Web3, contract_address: str, supports_erc721_metadata: bool
    ):
        contract = get_contract(self.db, contract_address)
        if contract is None:
            (name, symbol) = self._fetch_erc721_contract_metadata(
                w3, contract_address, supports_erc721_metadata
            )
            logger.info("Upserting ERC721 contract", name=name, symbol=symbol)
            upsert_contract(
                self.db,
                Contract(
                    address=contract_address,
                    name=name,
                    symbol=symbol,
                    contract_type=ContractType.ERC721.value,
                ),
            )

    def _fetch_erc721_contract_metadata(
        self, w3: Web3, contract_address: str, supports_erc721_metadata: bool
    ):
        if supports_erc721_metadata:
            try:
                erc721_contract = w3.eth.contract(
                    address=contract_address, abi=ERC721_ABI
                )
                return (
                    erc721_contract.functions.name().call(),
                    erc721_contract.functions.symbol().call(),
                )
            except Exception as e:
                return (None, None)
        return (None, None)

    def _upsert_nft(
        self,
        w3,
        contract_address: str,
        token_id: int,
        supports_erc721_metadata: bool,
    ):
        nft = get_nft(self.db, contract_address, token_id)
        if nft is None:
            token_uri = self._fetch_erc721_token_uri(
                w3, contract_address, token_id, supports_erc721_metadata
            )
            logger.info(
                "Upserting NFT",
                contract_address=contract_address,
                token_id=token_id,
                token_uri=token_uri,
            )
            upsert_nft(
                self.db,
                Nft(
                    contract_id=contract_address,
                    token_id=token_id,
                    token_uri=token_uri,
                ),
            )

    def _fetch_erc721_token_uri(
        self,
        w3: Web3,
        contract_address: str,
        token_id: int,
        supports_erc721_metadata: bool,
    ):
        if supports_erc721_metadata:
            try:
                erc721_contract = w3.eth.contract(
                    address=contract_address, abi=ERC721_ABI
                )
                return erc721_contract.functions.tokenURI(token_id).call()
            except Exception as e:
                return None
        return None

    def _upsert_ownerships(
        self,
        contract_address: str,
        token_id: int,
        transfer_from: str,
        transfer_to: str,
    ):
        nft_id = get_nft_id(contract_address, token_id)
        upsert_ownership(
            self.db,
            Ownership(
                nft_id=nft_id,
                owner_address=transfer_from,
                delta_quantity=-1,
            ),
        )
        upsert_ownership(
            self.db,
            Ownership(
                nft_id=nft_id, owner_address=transfer_to, delta_quantity=1
            ),
        )

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
