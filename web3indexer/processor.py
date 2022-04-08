from datetime import datetime
import json
from enum import Enum
from types import CoroutineType

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
    ERC1155 = "ERC1155"


ERC165_ABI = read_file("abi/ERC165.json")
ERC1155_ABI = read_file("abi/ERC1155.json")
ERC721_ABI = read_file("abi/ERC721.json")
ERC721_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
ERC1155_TRANSFER_SINGLE_TOPIC = (
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
)
ERC_165_IDENTIFIER = "01ffc9a7"
ERC_721_IDENTIFIER = "80ac58cd"
ERC_721_METADATA_IDENTIFIER = "5b5e139f"
ERC_1155_IDENTIFIER = "d9b67a26"
ERC_1155_METADATA_IDENTIFIER = "0e89341c"


class BlockProcessor:
    """
    Custom class for fetching, parsing and processing a block from the blockchain.
    """

    def __init__(self, db):
        self.db = db

    def process(self, dispatcher, w3, task):
        logger.info("Processing block", block_number=task.block_number)

        block = w3.eth.get_block(task.block_number)
        timestamp = datetime.fromtimestamp(block.timestamp)

        for transaction in block.transactions:
            txn_receipt = w3.eth.get_transaction_receipt(transaction)

            for log in txn_receipt.logs:
                self._process_log(w3, log, timestamp)

    def _process_log(self, w3: Web3, log, timestamp: datetime):
        topics = log.topics
        contract_address = log.address
        event_signature = topics[0].hex()
        if (
            event_signature == ERC721_TRANSFER_TOPIC
            and self._supports_interface(
                w3, contract_address, ERC_721_IDENTIFIER
            )
        ):
            self._process_erc721_log(w3, log, timestamp)
        elif (
            event_signature == ERC1155_TRANSFER_SINGLE_TOPIC
            and self._supports_interface(
                w3, contract_address, ERC_1155_IDENTIFIER
            )
        ):
            self._process_erc1155_transfer_single_log(w3, log, timestamp)

    def _process_erc721_log(self, w3: Web3, log, timestamp: datetime):
        contract_address = log.address
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
            transaction_hash=transaction_hash,
        )
        supports_erc721_metadata = self._supports_interface(
            w3, contract_address, ERC_721_METADATA_IDENTIFIER
        )
        self._upsert_contract(
            w3, contract_address, ContractType.ERC721, supports_erc721_metadata
        )
        self._upsert_nft(
            w3,
            contract_address,
            token_id,
            ContractType.ERC721,
            supports_erc721_metadata,
        )
        upsert_transfer(
            self.db,
            Transfer(
                nft_id=get_nft_id(contract_address, token_id),
                quantity=1,
                timestamp=timestamp,
                transaction_hash=transaction_hash,
                transfer_from=transfer_from,
                transfer_to=transfer_to,
            ),
        )
        self._upsert_ownerships(
            contract_address, token_id, transfer_from, transfer_to, 1
        )

    def _process_erc1155_transfer_single_log(
        self, w3: Web3, log, timestamp: datetime
    ):
        contract_address = log.address
        (
            transfer_from,
            transfer_to,
            token_id,
            quantity,
            transaction_hash,
        ) = self._parse_erc1155_transfer_single_log(log)
        logger.info(
            "Processing ERC1155 transfer single",
            transfer_from=transfer_from,
            transfer_to=transfer_to,
            transaction_hash=transaction_hash,
        )
        supports_erc1155_metadata = self._supports_interface(
            w3, contract_address, ERC_1155_METADATA_IDENTIFIER
        )
        self._upsert_contract(
            w3,
            contract_address,
            ContractType.ERC1155,
            supports_erc1155_metadata,
        )
        self._upsert_nft(
            w3,
            contract_address,
            token_id,
            ContractType.ERC1155,
            supports_erc1155_metadata,
        )
        upsert_transfer(
            self.db,
            Transfer(
                nft_id=get_nft_id(contract_address, token_id),
                quantity=quantity,
                timestamp=timestamp,
                transaction_hash=transaction_hash,
                transfer_from=transfer_from,
                transfer_to=transfer_to,
            ),
        )
        self._upsert_ownerships(
            contract_address, token_id, transfer_from, transfer_to, quantity
        )

    def _upsert_contract(
        self,
        w3: Web3,
        contract_address: str,
        contract_type: ContractType,
        supports_metadata: bool,
    ):
        contract = get_contract(self.db, contract_address)
        if contract is not None:
            return

        if contract_type == ContractType.ERC721:
            (name, symbol) = self._fetch_erc721_contract_metadata(
                w3, contract_address, supports_metadata
            )
            logger.info(
                "Upserting ERC721 contract",
                name=name,
                symbol=symbol,
                address=contract_address,
            )
            upsert_contract(
                self.db,
                Contract(
                    address=contract_address,
                    name=name,
                    symbol=symbol,
                    contract_type=ContractType.ERC721.value,
                ),
            )
        elif contract_type == ContractType.ERC1155:
            logger.info(
                "Upserting ERC1155 contract",
                address=contract_address,
            )
            upsert_contract(
                self.db,
                Contract(
                    address=contract_address,
                    contract_type=ContractType.ERC1155.value,
                ),
            )
        else:
            raise Exception(
                "Unhandled contract type in `_upsert_contract`: {}".format(
                    contract_type
                )
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
        contract_type: ContractType,
        supports_metadata: bool,
    ):
        nft = get_nft(self.db, contract_address, token_id)
        if nft is not None:
            return

        if contract_type == ContractType.ERC721:
            token_uri = self._fetch_erc721_token_uri(
                w3, contract_address, token_id, supports_metadata
            )
            logger.info(
                "Upserting ERC721 NFT",
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
        elif contract_type == ContractType.ERC1155:
            token_uri = self._fetch_erc1155_token_uri(
                w3, contract_address, token_id, supports_metadata
            )
            logger.info(
                "Upserting ERC1155 NFT",
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
        else:
            raise Exception(
                "Unhandled contract type in `_upsert_nft`: {}".format(
                    contract_type
                )
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

    def _fetch_erc1155_token_uri(
        self,
        w3: Web3,
        contract_address: str,
        token_id: int,
        supports_metadata: bool,
    ):
        if supports_metadata:
            try:
                erc1155_contract = w3.eth.contract(
                    address=contract_address, abi=ERC1155_ABI
                )
                return erc1155_contract.functions.uri(token_id).call()
            except Exception as e:
                return None
        return None

    def _upsert_ownerships(
        self,
        contract_address: str,
        token_id: int,
        transfer_from: str,
        transfer_to: str,
        quantity: int,
    ):
        nft_id = get_nft_id(contract_address, token_id)
        upsert_ownership(
            self.db,
            Ownership(
                nft_id=nft_id,
                owner_address=transfer_from,
                delta_quantity=-1 * quantity,
            ),
        )
        upsert_ownership(
            self.db,
            Ownership(
                nft_id=nft_id,
                owner_address=transfer_to,
                delta_quantity=quantity,
            ),
        )

    def _parse_erc721_transfer_log(self, log):
        topics = log.topics
        transfer_from = "0x{}".format(topics[1].hex()[26:])
        transfer_to = "0x{}".format(topics[2].hex()[26:])
        token_id = int(topics[3].hex(), 16)
        transaction_hash = log.transactionHash.hex()
        return (transfer_from, transfer_to, token_id, transaction_hash)

    def _parse_erc1155_transfer_single_log(self, log):
        topics = log.topics
        transfer_from = "0x{}".format(topics[2].hex()[26:])
        transfer_to = "0x{}".format(topics[3].hex()[26:])

        data = log.data
        token_id = int(data[2:66], 16)
        quantity = int(data[66:], 16)

        transaction_hash = log.transactionHash.hex()

        return (
            transfer_from,
            transfer_to,
            token_id,
            quantity,
            transaction_hash,
        )

    def _supports_interface(
        self, w3: Web3, contract_address: str, interface: str
    ):
        try:
            erc165_contract = w3.eth.contract(
                address=contract_address, abi=ERC165_ABI
            )
            return erc165_contract.functions.supportsInterface(interface).call()
        except Exception as e:
            return False
