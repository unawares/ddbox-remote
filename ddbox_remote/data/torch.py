from typing import List

from torch.utils.data import Dataset

from ddbox_remote.data.utils.loaders import MosesDataLoader

__all__ = ['MosesDataset']


class MosesDataset(Dataset):

    def __init__(self, split: str = 'train', attributes: List[str] = ['smiles'], transform=None):
        self.data_loader = MosesDataLoader(split, attributes)
        self.transform = transform
        self.data_loader.download()

    def __len__(self):
        return self.data_loader.get_total()

    def __getitem__(self, idx):
        if isinstance(idx, slice):
            total = self.data_loader.get_total()
            start = idx.start if idx.start is not None else 0
            start = start if start >= 0 else total + start
            stop = idx.stop if idx.stop is not None else total
            stop = stop if stop >= 0 else total + stop
            step = idx.step if idx.step is not None else 1
            if start < 0 or start >= total:
                raise Exception("Index out of range")
            if stop < 0 or stop > total:
                raise Exception("Index out of range")
            if start > stop:
                raise Exception("Invalid range")
            limit = stop - start

            if limit == 0:
                return []

            items = self.data_loader.get(start, limit)
            filtered = []
            for i in range(0, stop - start, step):
                filtered.append(items[i])
            if self.transform:
                filtered = self.transform(filtered)
            return filtered
        else:
            item = self.data_loader.get(idx, 1)
            if self.transform:
                item = self.transform(item)
            return item
