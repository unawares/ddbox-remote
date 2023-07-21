import json
import uuid
from typing import *

import base64
import os
import errno
import shutil

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes


def base64_encode(message_bytes: bytes) -> bytes:
    base64_bytes = base64.b64encode(message_bytes)
    return base64_bytes


def base64_decode(message_base64_bytes: bytes) -> bytes:
    message_bytes = base64.b64decode(message_base64_bytes)
    return message_bytes


def get_hash(plain: bytes) -> bytes:
    digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
    digest.update(plain)
    return digest.finalize()


def json_to_pretty_str(json_data: Mapping[str, Any]):
    json_formatted_str = json.dumps(json_data, indent=2)
    return json_formatted_str


def remove_path(path):
    if os.path.isfile(path) or os.path.islink(path):
        os.remove(path)  # remove the file
    elif os.path.isdir(path):
        shutil.rmtree(path)  # remove dir and all contains
    else:
        pass


def exists_file(path):
    return os.path.isfile(path)


def ensure_path(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def get_random_uuid_hex():
    return uuid.uuid4().hex
