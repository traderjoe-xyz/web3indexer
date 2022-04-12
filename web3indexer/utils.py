from .models import Transfer


def get_nft_id(contract_address: str, token_id: int):
    return "{}-{}".format(contract_address, token_id)


def get_transfer_id(transfer: Transfer):
    # We need to include the `nft_id` in order to avoid conflicts because
    # an ERC1155 `TransferBatch` event contains multiple transfers
    return "{}-{}-{}".format(
        transfer.txn_hash, transfer.log_index, transfer.nft_id
    )


def read_file(path):
    with open(path) as fh:
        return fh.read()
