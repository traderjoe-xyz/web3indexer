from datetime import datetime

from pymongo.database import Database
import structlog
from web3 import Web3

from .constants import (
    ERC721_TRANSFER_TOPIC,
    ERC1155_TRANSFER_SINGLE_TOPIC,
    ERC1155_TRANSFER_BATCH_TOPIC,
    ERC_721_IDENTIFIER,
    ERC_721_METADATA_IDENTIFIER,
    ERC_1155_IDENTIFIER,
    ERC_1155_METADATA_IDENTIFIER,
)
from .crud import (
    get_contract,
    get_nft,
    upsert_contract,
    upsert_ownership,
    upsert_nft,
    upsert_transfer,
)
from .dispatcher import Dispatcher
from .models import Contract, ContractType, Nft, Transfer, UpsertOwnership
from .task import ProcessLogTask
from .utils import get_nft_id, read_file


logger = structlog.get_logger()

ERC165_ABI = read_file("abi/ERC165.json")
ERC1155_ABI = read_file("abi/ERC1155.json")
ERC721_ABI = read_file("abi/ERC721.json")


class LogProcessor:
    """
    Handles fetching, parsing and storing information for a given block.
    """

    MAX_RETRIES = 5

    def __init__(self, w3: Web3, db: Database):
        self.w3 = w3
        self.db = db

    def process_with_retry(
        self, block_number: int, log, log_index: int, timestamp: datetime
    ):
        try:
            self.process(block_number, log, log_index, timestamp)
        except Exception as e:
            # TODO: Handle retries
            logger.error(e)

    def process(
        self, block_number: int, log, log_index: int, timestamp: datetime
    ):
        logger.info(
            "Fetching log",
            block_number=block_number,
            log_index=log_index,
        )

        topics = log.topics
        contract_address = log.address
        event_signature = topics[0]
        if (
            event_signature == ERC721_TRANSFER_TOPIC
            and self._supports_interface(
                self.w3, contract_address, ERC_721_IDENTIFIER
            )
        ):
            self._process_erc721_log(
                self.w3, log, block_number, log_index, timestamp
            )
        elif (
            event_signature == ERC1155_TRANSFER_SINGLE_TOPIC
            or event_signature == ERC1155_TRANSFER_BATCH_TOPIC
        ) and self._supports_interface(
            self.w3, contract_address, ERC_1155_IDENTIFIER
        ):
            if event_signature == ERC1155_TRANSFER_SINGLE_TOPIC:
                self._process_erc1155_transfer_single_log(
                    self.w3, log, block_number, log_index, timestamp
                )
            elif event_signature == ERC1155_TRANSFER_BATCH_TOPIC:
                self._process_erc1155_transfer_batch_log(
                    self.w3, log, block_number, log_index, timestamp
                )

    def _process_erc721_log(
        self,
        w3: Web3,
        log,
        block_number: int,
        log_index: int,
        timestamp: datetime,
    ):
        contract_address = log.address
        (
            transfer_from,
            transfer_to,
            token_id,
            txn_hash,
        ) = self._parse_erc721_transfer_log(log)
        logger.info(
            "Processing ERC721 transfer",
            txn_hash=txn_hash,
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
                log_index=log_index,
                nft_id=get_nft_id(contract_address, token_id),
                quantity=1,
                timestamp=timestamp,
                txn_hash=txn_hash,
                transfer_from=transfer_from,
                transfer_to=transfer_to,
            ),
        )
        self._upsert_ownerships(
            contract_address,
            token_id,
            transfer_from,
            transfer_to,
            1,  # quantity
            block_number,
            log_index,
        )

    def _process_erc1155_transfer_single_log(
        self,
        w3: Web3,
        log,
        block_number: int,
        log_index: int,
        timestamp: datetime,
    ):
        contract_address = log.address
        (
            transfer_from,
            transfer_to,
            token_id,
            quantity,
            txn_hash,
        ) = self._parse_erc1155_transfer_single_log(log)
        logger.info(
            "Processing ERC1155 transfer single",
            txn_hash=txn_hash,
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
                log_index=log_index,
                nft_id=get_nft_id(contract_address, token_id),
                quantity=quantity,
                timestamp=timestamp,
                txn_hash=txn_hash,
                transfer_from=transfer_from,
                transfer_to=transfer_to,
            ),
        )
        self._upsert_ownerships(
            contract_address,
            token_id,
            transfer_from,
            transfer_to,
            quantity,
            block_number,
            log_index,
        )

    def _process_erc1155_transfer_batch_log(
        self,
        w3: Web3,
        log,
        block_number: int,
        log_index: int,
        timestamp: datetime,
    ):
        contract_address = log.address
        (
            transfer_from,
            transfer_to,
            token_ids,
            quantities,
            txn_hash,
        ) = self._parse_erc1155_transfer_batch_log(log)
        logger.info(
            "Processing ERC1155 transfer batch",
            txn_hash=txn_hash,
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

        # We expect there to be same number of `token_ids` as `quantities`
        for i in range(len(token_ids)):
            token_id = token_ids[i]
            quantity = quantities[i]

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
                    log_index=log_index,
                    nft_id=get_nft_id(contract_address, token_id),
                    quantity=quantity,
                    timestamp=timestamp,
                    txn_hash=txn_hash,
                    transfer_from=transfer_from,
                    transfer_to=transfer_to,
                ),
            )
            self._upsert_ownerships(
                contract_address,
                token_id,
                transfer_from,
                transfer_to,
                quantity,
                block_number,
                log_index,
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

    def _parse_erc721_transfer_log(self, log):
        topics = log.topics
        transfer_from = "0x{}".format(topics[1][26:])
        transfer_to = "0x{}".format(topics[2][26:])
        token_id = int(topics[3], 16)
        txn_hash = log.transactionHash
        return (transfer_from, transfer_to, token_id, txn_hash)

    def _parse_erc1155_transfer_single_log(self, log):
        topics = log.topics
        transfer_from = "0x{}".format(topics[2][26:])
        transfer_to = "0x{}".format(topics[3][26:])

        data = log.data
        token_id = int(data[2:66], 16)
        quantity = int(data[66:], 16)

        txn_hash = log.transactionHash

        return (
            transfer_from,
            transfer_to,
            token_id,
            quantity,
            txn_hash,
        )

    def _parse_erc1155_transfer_batch_log(self, log):
        # See https://ethereum.stackexchange.com/questions/58854/how-to-decode-data-parameter-under-logs-in-transaction-receipt
        topics = log.topics
        transfer_from = "0x{}".format(topics[2][26:])
        transfer_to = "0x{}".format(topics[3][26:])

        # Strip the prefixing `0x`
        data = log.data[2:]
        ids_offset = int(data[0:64], 16)
        values_offset = int(data[64:128], 16)

        ids_start = ids_offset + 64
        quantities_start = values_offset + 128

        num_ids = int(data[ids_start : ids_start + 64], 16)
        num_quantities = int(data[quantities_start : quantities_start + 64], 16)

        if num_ids != num_quantities:
            raise Exception(
                "Expected same number of ids and quantities for TransferBatch event log"
            )

        ids_contents_start = ids_start + 64
        quantities_contents_start = quantities_start + 64

        ids = []
        quantities = []

        for i in range(num_ids):
            start_index = ids_contents_start + i * 64
            end_index = start_index + 64
            ids.append(int(data[start_index:end_index], 16))

        for i in range(num_quantities):
            start_index = quantities_contents_start + i * 64
            end_index = start_index + 64
            quantities.append(int(data[start_index:end_index], 16))

        txn_hash = log.transactionHash

        return (
            transfer_from,
            transfer_to,
            ids,
            quantities,
            txn_hash,
        )

    def _upsert_ownerships(
        self,
        contract_address: str,
        token_id: int,
        transfer_from: str,
        transfer_to: str,
        quantity: int,
        block_number: int,
        log_index: int,
    ):
        nft_id = get_nft_id(contract_address, token_id)
        upsert_ownership(
            self.db,
            UpsertOwnership(
                block_number=block_number,
                delta_quantity=-1 * quantity,
                log_index=log_index,
                nft_id=nft_id,
                owner=transfer_from,
            ),
        )
        upsert_ownership(
            self.db,
            UpsertOwnership(
                block_number=block_number,
                delta_quantity=quantity,
                log_index=log_index,
                nft_id=nft_id,
                owner=transfer_to,
            ),
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
