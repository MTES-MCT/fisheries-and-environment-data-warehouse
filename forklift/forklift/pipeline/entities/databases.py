from enum import Enum


class Database(Enum):
    MONITORFISH_PROXY = "monitorfish_proxy"
    MONITORENV_PROXY = "monitorenv_proxy"
    RAPPORTNAV_PROXY = "rapportnav_proxy"
    MONITORFISH = "monitorfish"
    MONITORENV = "monitorenv"
    RAPPORTNAV = "rapportnav"
    SACROIS = "sacrois"
