from dataclasses import dataclass
from typing import Union


@dataclass
class IdRange:
    id_min: Union[int, str]
    id_max: Union[int, str]
