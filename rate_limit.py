from slowapi import Limiter
from slowapi.util import get_remote_address

# Single shared Limiter instance — imported by main.py and any router that
# needs @limiter.limit() decorators.
limiter = Limiter(key_func=get_remote_address)
