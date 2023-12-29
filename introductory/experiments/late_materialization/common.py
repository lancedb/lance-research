import lance
from typing import List

def scan_lance(
    ds: lance.LanceDataset,
    columns: List[str],
    predicate: str,
    late_materialization: bool,
) -> int:
    """
    Scans a Lance dataset, returning the number of rows.
    """
    reader = ds.scanner(
        columns=columns,
        filter=predicate,
        use_late_materialization=late_materialization,
    ).to_batches()
    num_rows = 0
    for batch in reader:
        num_rows += batch.num_rows
    return num_rows
