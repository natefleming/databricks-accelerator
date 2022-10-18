from typing import Any
from abc import ABC, abstractmethod


class _HelpMixin(ABC):

    @abstractmethod
    def help(self) -> None:
        pass
