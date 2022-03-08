import structlog


log = structlog.get_logger()


def insert_if_not_exists(db, address, abi):
    contract = db.nft_contracts.find_one({'address': address})
    if contract is not None:
        return contract['_id']
    return db.nft_contracts.insert_one({
        "address": address,
        "abi": abi,
        "events": [],
    }).inserted_id


def insert_event(db, address, event):
    """
    Insert an event into mongodb
    """
    contract_id = db.nft_contracts.find_one({'address': address})['_id']
    db.nft_contracts.update_one(
        {'_id': contract_id}, {"$push": {"events": event}}
    )


def get_last_scanned_event(db, address, default=9000000):
    """
    Return the block number the last event
    was added at.
    """
    event = db.nft_contracts.find_one({
        'address': address,
    })
    # Inneficient XXX
    if not event['events']:
        return default
    return event['events'][-1]['blockNumber']


def get_all_contracts(db):
    return db.nft_contracts.find({}, {'address': 1, 'abi': 1, '_id': 0})
