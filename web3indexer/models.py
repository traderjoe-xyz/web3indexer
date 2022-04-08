from typing import Optional

from pydantic import BaseModel, Field


class Contract(BaseModel):
    address: str = Field(alias="_id")
    name: Optional[str]
    symbol: Optional[str]
    contract_type: str = Field(alias="type")

    class Config:
        allow_population_by_field_name = True


class Transfer(BaseModel):
    contract: str
    token_id: int
    transaction_hash: str = Field(alias="_id")
    transfer_from: str = Field(alias="from")
    transfer_to: str = Field(alias="to")

    class Config:
        allow_population_by_field_name = True
