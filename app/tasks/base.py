from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..workflow import WorkflowDef


class Status(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Task(ABC):
    name: str = "Unnamed task"
    status: Status = Status.PENDING
    detail: str = None

    @abstractmethod
    def run(self, *args, context: "WorkflowDef", **kwargs):
        raise NotImplementedError
    
    def postrun(self, *args, context: "WorkflowDef", **kwargs):
        pass


