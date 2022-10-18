from typing import Any
from abc import ABC, abstractmethod

from honeycomb._command._HelpMixin import _HelpMixin
from honeycomb._command._Nameable import _Nameable


class _Command(_Nameable, _HelpMixin):

    def __init__(self, name: str):
        super().__init__(name)
