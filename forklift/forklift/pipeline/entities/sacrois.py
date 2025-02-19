from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path


class SacroisFileType(Enum):
    NAVIRES_MOIS_MAREES_JOUR = "NAVIRES_MOIS_MAREES_JOUR"
    REJETS = "REJETS"
    BMS = "BMS"
    FISHING_ACTIVITY = "FISHING_ACTIVITY"

    def to_table_name(self):
        return self.value.lower()


@dataclass
class SacroisFileImportSpec:
    filetype: SacroisFileType
    year: int
    month: int
    partition: str
    filepath: Path


@dataclass
class SacroisPartition:
    name: str
    processing_datetime: datetime


@dataclass
class IdRange:
    id_min: int
    id_max: int
