from typing import List, Optional

import numpy as np


class BatchTokenReader:
    def __init__(
            self,
            tokens: List[List[int]], data_paths: List[str],
            batch_size: int, seq_len: int,
            bucket_count: int, queue_size: int,
    ): ...

    def __iter__(self) -> BatchTokenReader: ...

    def __next__(self) -> Optional[np.array]: ...
