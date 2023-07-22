import csv
import os
from typing import List

import requests
from tqdm import tqdm

from ddbox_remote.configs import BASE_API_URL, DATA_CACHE
from ddbox_remote.utils import ensure_path, exists_file, get_hash, remove_path

__all__ = ['MosesDataLoader']


MOSES_INFO_API_URL = BASE_API_URL + '/data/molecules/info/'
MOSES_API_URL = BASE_API_URL + '/data/molecules/'


class MosesDataLoader:

    tags: List[str]
    attributes: List[str]
    cache_dir: str
    batch_size: int
    _info: dict

    def __init__(self,
                 split: str = 'train',
                 attributes: List[str] = ['smiles'],
                 batch_size: int = 50000,
                 cache_dir=os.path.join(DATA_CACHE, 'moses'),
                 ) -> None:
        self.tags = [split, 'moses']
        self.attributes = attributes
        self.cache_dir = cache_dir
        self.batch_size = batch_size
        self._info = None

    def _load_info(self):
        response = requests.get(MOSES_INFO_API_URL, params={
            'tags': ','.join(sorted(set(self.tags))),
            'attributes': ','.join(sorted(set(self.attributes))),
        })

        if response.status_code != 200:
            raise Exception(response.text)

        self._info = response.json()

    def _get_cache_path(self):
        text = ''.join(sorted([str(self.batch_size), *self.tags, *self.attributes]))
        h = get_hash(text.encode()).hex()
        return os.path.join(self.cache_dir, h)

    def _get_cache_file_path(self, filename):
        cache_path = self._get_cache_path()
        return os.path.join(cache_path, filename)

    def _fetch(self, offset: int, limit: int):
        sorted_attributes = sorted(set(self.attributes))

        response = requests.get(MOSES_API_URL, params={
            'offset': offset,
            'limit': limit,
            'tags': ','.join(sorted(set(self.tags))),
            'attributes': ','.join(sorted_attributes),
        })

        if response.status_code != 200:
            raise Exception(response.text)

        sorted_records = response.json()['records']

        records = []

        for sorted_record in sorted_records:
            record = []
            for attribute in self.attributes:
                index = sorted_attributes.index(attribute)
                record.append(sorted_record[index])
            records.append(record)

        return records

    def _check_if_downloaded(self):
        limit = self.batch_size
        total = self.get_total()

        i = 0
        for _ in range(0, total, limit):
            i += 1

            if not exists_file(self._get_cache_file_path('%s.csv' % i)):
                return False

        return True

    def download(self):
        if not self._check_if_downloaded():
            cache_path = self._get_cache_path()

            remove_path(cache_path)
            ensure_path(cache_path)

            limit = self.batch_size
            total = self.get_total()

            i = 0

            for offset in tqdm(range(0, total, limit)):
                i += 1
                records = self._fetch(offset, limit)

                with open(self._get_cache_file_path('%s.csv' % i), 'w') as f:
                    writer = csv.writer(f)
                    writer.writerows(records)

    def get_total(self):
        if self._info is None:
            self._load_info()
        return self._info['total']

    def get(self, offset: int, limit: int):
        if offset >= self.get_total():
            raise Exception("Offset is out of range")

        if offset + limit > self.get_total():
            raise Exception("Limit is out of range")

        if self.download:
            records = []

            si = offset // self.batch_size
            ei = (limit + offset - 1) // self.batch_size

            for i in range(si, ei + 1):
                with open(self._get_cache_file_path('%s.csv' % (i + 1)), 'r') as f:
                    reader = csv.reader(f)
                    rows = []
                    for row in reader:
                        rows.append(row)

                    start_idx = 0
                    end_idx = len(rows)

                    if i == si:
                        start_idx = offset - si * self.batch_size
                    if i == ei:
                        end_idx = (limit + offset) - ei * self.batch_size

                    records.extend(rows[start_idx:end_idx])

            return records
        else:
            return self._fetch(offset, limit)
