from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ContractType(Enum):
    ERC721 = "ERC721"
    ERC1155 = "ERC1155"


class Contract(BaseModel):
    address: str = Field(alias="_id")
    name: Optional[str]
    symbol: Optional[str]
    contract_type: str = Field(alias="type")

    class Config:
        allow_population_by_field_name = True


class Nft(BaseModel):
    contract_id: str
    token_id: int
    token_uri: Optional[str]


class Transfer(BaseModel):
    log_index: int
    nft_id: str
    quantity: int
    timestamp: datetime
    transfer_from: str = Field(alias="from")
    transfer_to: str = Field(alias="to")
    txn_hash: str

    class Config:
        allow_population_by_field_name = True


# TODO: How can we model this so that we can re-process a block
# without any corruption in data?
class Ownership(BaseModel):
    nft_id: str
    owner_address: str
    delta_quantity: int
