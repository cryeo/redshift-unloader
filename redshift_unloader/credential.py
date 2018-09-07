from typing import NamedTuple, Dict, Optional


class Credential(NamedTuple):
    access_key_id: str
    secret_access_key: str

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "ACCESS_KEY_ID": self.access_key_id,
            "SECRET_ACCESS_KEY": self.secret_access_key
        }
