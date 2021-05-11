"""
Primeight module with utils.

    Classes:
        UUIDEncoder: JSON encoder that handles UUIDs properly

"""

import json
from uuid import UUID


class UUIDEncoder(json.JSONEncoder):
    """JSON encoder that handles UUID objects"""

    def default(self, o):
        if isinstance(o, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return str(o)
        return json.JSONEncoder.default(self, o)
