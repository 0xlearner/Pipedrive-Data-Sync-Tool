from enum import Enum
from typing import Optional
from pydantic import BaseModel


class StageStatus(Enum):
    TRYING_TO_CONTACT = 0
    TOOK_APP = 1
    REC_DOCS_LENDER_CALL = 2
    FINANCIAL_SCHED = 3
    COMPLIANCE_SCHED = 4
    PENDING_PAYMENT = 5
    PAID = 6
    SUBMITTED_TO_PROCESSING = 7

    @property
    def description(self):
        descriptions = {
            0: "Trying to Contact",
            1: "Took App",
            2: "Rec Docs - Lender Call",
            3: "Financial Sched",
            4: "Compliance Shed",
            5: "Pending Payment",
            6: "PAID",
            7: "Sub'd to Processing",
        }
        return descriptions[self.value]

    @classmethod
    def from_number(cls, number):
        return cls(number)

    def to_dict(self):
        return {"value": self.value, "description": self.description}


# Pydantic model for person data
class PersonInfo(BaseModel):
    id: str
    benefit_id: Optional[str] = None
    phone_number: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None


# Pydantic model for deal data
class DealInfo(BaseModel):
    id: str
    person_id: str
    stage_status: Optional[StageStatus]
    status: str
    assigned_to: Optional[str] = None
    updated_at: Optional[str] = None
    name: Optional[str] = None
