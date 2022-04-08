from .models import Transfer


def get_nft_id(contract_address: str, token_id: int):
    return "{}-{}".format(contract_address, token_id)


def get_transfer_id(transfer: Transfer):
    return "{}-{}".format(
        transfer.txn_hash,
        transfer.log_index,
    )


def read_file(path):
    with open(path) as fh:
        return fh.read()
