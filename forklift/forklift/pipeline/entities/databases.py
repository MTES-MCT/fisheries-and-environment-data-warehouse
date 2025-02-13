from enum import Enum


class Database(Enum):
    MONITORFISH_PROXY = "monitorfish_proxy"
    MONITORENV_PROXY = "monitorenv_proxy"
    MONITORFISH = "monitorfish"
    MONITORENV = "monitorenv"
    SACROIS = "sacrois"
