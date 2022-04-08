from lib2to3.pgen2 import token
import structlog

from pymongo.database import Database

from .models import Contract, Nft, Transfer


log = structlog.get_logger()


def insert_if_not_exists(db: Database, address, abi):
    contract = db.nft_contracts.find_one({"address": address})
    if contract is None:
        db.nft_contracts.insert_one(
            {
                "address": address,
                "abi": abi,
            }
        )


def insert_event(db: Database, address, event):
    """
    Insert an event into mongodb
    """
    contract_id = db.nft_contracts.find_one({"address": address})["_id"]
    db.nft_events.insert_one(
        {
            "address": address,
            "name": event["event"],
            "event": event,
            "blockNumber": event["blockNumber"],
            "tokenId": event["args"].get("tokenId"),
        }
    )


def get_last_scanned_event(db: Database, address, default=9000000):
    """
    Return the block number the last event
    was added at.
    """
    event = db.nft_events.find_one(
        {"$query": {"address": address}, "$orderby": {"blockNumber": -1}},
    )
    # Inneficient XXX
    if event is None:
        return default
    return event["blockNumber"]


def get_contract(db: Database, address):
    return db.contracts.find_one({"_id": address})


def upsert_contract(db: Database, contract: Contract):
    """
    Insert a contract into mongodb
    """
    db.contracts.find_one_and_update(
        {"_id": contract.address},
        {"$set": contract.dict(by_alias=True)},
        upsert=True,
    )


def get_nft(db: Database, address: str, token_id: int):
    return db.nfts.find_one({"_id": "{}-{}".format(address, token_id)})


def upsert_nft(db: Database, nft: Nft):
    nft_id = "{}-{}".format(nft.contract, nft.token_id)
    return db.nfts.find_one_and_update(
        {"_id": nft_id},
        {"$set": {"_id": nft_id, **nft.dict()}},
        upsert=True,
    )


def upsert_transfer(db: Database, transfer: Transfer):
    """
    Insert a transfer event into mongodb
    """
    db.transfers.find_one_and_update(
        {"_id": transfer.transaction_hash},
        {"$set": transfer.dict(by_alias=True)},
        upsert=True,
    )


def get_all_contracts(db):
    return db.nft_contracts.find({}, {"address": 1, "abi": 1, "_id": 0})
