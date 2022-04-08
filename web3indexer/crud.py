import structlog

from .models import Contract, Transfer


log = structlog.get_logger()


def insert_if_not_exists(db, address, abi):
    contract = db.nft_contracts.find_one({"address": address})
    if contract is None:
        db.nft_contracts.insert_one(
            {
                "address": address,
                "abi": abi,
            }
        )


def insert_event(db, address, event):
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


def get_last_scanned_event(db, address, default=9000000):
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


def get_contract(db, address):
    return db.contracts.find_one({"_id": address})


def upsert_contract(db, contract: Contract):
    """
    Insert a contract into mongodb
    """
    db.contracts.find_one_and_update(
        {"_id": contract.address},
        {"$set": contract.dict(by_alias=True)},
        upsert=True,
    )


def upsert_transfer(db, transfer: Transfer):
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
