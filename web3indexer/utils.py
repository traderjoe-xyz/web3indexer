def get_nft_id(contract_address: str, token_id: int):
    return "{}-{}".format(contract_address, token_id)


def read_file(path):
    with open(path) as fh:
        return fh.read()
