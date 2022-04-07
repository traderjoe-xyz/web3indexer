import structlog


log = structlog.get_logger()


def insert_if_not_exists(db, address, abi):
    contract = db.nft_contracts.find_one({'address': address})
    if contract is None:
        db.nft_contracts.insert_one({
            "address": address,
            "abi": abi,
        })


def insert_event(db, address, event):
    """
    Insert an event into mongodb
    """
    contract_id = db.nft_contracts.find_one({'address': address})['_id']
    db.nft_events.insert_one({
        'address': address,
        "name": event['event'],
        "event": event,
        "blockNumber": event['blockNumber'],
        "tokenId": event['args'].get('tokenId'),
    })


def get_last_scanned_event(db, address, default=9000000):
    """
    Return the block number the last event
    was added at.
    """
    event = db.nft_events.find_one({
        "$query": {'address': address},
        "$orderby": {"blockNumber": -1}
    }, )
    # Inneficient XXX
    if event is None:
        return default
    return event['blockNumber']


def insert_transfer(db, transfer):
    """
    Insert a transfer event into mongodb
    """
    db.transfers.insert_one({
        "from": transfer["from"],
        "nft_contract": transfer["nft_contract"],
        "to": transfer["to"],
        "tokenId": transfer["tokenId"],
        "transactionHash": transfer["transactionHash"],
    })


def get_all_contracts(db):
    return db.nft_contracts.find({}, {'address': 1, 'abi': 1, '_id': 0})
