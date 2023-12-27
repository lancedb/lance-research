import os
import pathlib
import urllib.request

import lance
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from dataclasses import dataclass

# This adapter for urllib.request.urlretrieve comes from the tqdm usage guide
class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""
    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        return self.update(b * bsize - self.n)  # also sets self.n = b * bsize

@dataclass
class DownloadParams:
    '''
    The command-line parameters
    '''
    working_directory: pathlib.Path
    dest_parquet_dataset: pathlib.Path
    target_num_rows: int

def get_metadata_url(shard_number: int):
    '''URL of the metadata file'''
    return f"https://the-eye.eu/public/AI/cah/laion5b/embeddings/laion2B-en/laion2B-en-metadata/metadata_{shard_number:04}.parquet"

def get_embedding_url(shard_number: int):
    '''URL of the embedding file'''
    return f"https://the-eye.eu/public/AI/cah/laion5b/embeddings/laion2B-en/img_emb/img_emb_{shard_number:04}.npy"

def get_embedding_working_path(shard_number: int, params: DownloadParams):
    '''Temporary path for downloading the embedding'''
    return params.working_directory / f"embedding_{shard_number:04}.npy"

def get_metadata_working_path(shard_number: int, params: DownloadParams):
    '''Temporary path for downloading the metadata'''
    return params.working_directory / f"meta_{shard_number:04}.parquet"

def get_shard_dest_path(shard_number: int, params: DownloadParams):
    '''The path of a shard in our parquet dataset'''
    return params.dest_parquet_dataset / f"shard_{shard_number:04}.parquet"

def download_file_if_needed(src: pathlib.Path, dest: pathlib.Path, title: str):
    '''Downloads a file from the internet, if it does not already exist'''
    if os.path.exists(dest):
        return
    # We download to .partial first and then rename.  This way, if the script is interrupted
    # during the download, we won't assume the partially downloaded file is complete
    partial_dest = dest.parent / (dest.name + ".partial")
    bar_format = f'{title}: ' + '{l_bar}{bar}{r_bar}'
    with TqdmUpTo(unit="B", unit_scale=True, mininterval=5.0, bar_format=bar_format) as progress:
        urllib.request.urlretrieve(src, partial_dest, progress.update_to)
    os.replace(partial_dest, dest)


def download_shard(shard_number: int, params: DownloadParams):
    '''Downloads a shard of data from the internet into our working directory'''
    metadata_url = get_metadata_url(shard_number)
    metadata_dest = get_metadata_working_path(shard_number, params)
    download_file_if_needed(metadata_url, metadata_dest, f'Shard {shard_number} metadata ')

    embedding_url = get_embedding_url(shard_number)
    embedding_dest = get_embedding_working_path(shard_number, params)
    download_file_if_needed(embedding_url, embedding_dest, f'Shard {shard_number} embedding')

    return pq.read_metadata(metadata_dest).num_rows

def load_embedding(shard_number: int, params: DownloadParams):
    '''
    Load an embedding file from our working directory as a pyarrow array
    '''
    embedding_path = get_embedding_working_path(shard_number, params)
    embedding_arr = np.load(embedding_path)
    # The laion-5B dataset uses float16 for embeddings.  Parquet does not support this and
    # so we convert to float32
    embedding_arr = embedding_arr.astype(np.single)
    width = embedding_arr.shape[1]
    embedding_arr.shape = -1
    return pa.FixedSizeListArray.from_arrays(embedding_arr, width)

def load_metadata(shard_number: int, params: DownloadParams):
    '''
    Load a metadata file from our working directory as a pyarrow table
    '''
    metadata_path = get_metadata_working_path(shard_number, params)
    return pq.read_table(metadata_path)

def cleanup_temp_files(shard_number: int, params: DownloadParams):
    '''
    Remove the files from our working directory to free up space once they are joined
    '''
    def try_delete(path):
        try:
            os.remove(path)
        except OSError:
            pass
    try_delete(get_embedding_working_path(shard_number, params))
    try_delete(get_metadata_working_path(shard_number, params))


def download_and_convert_shard(shard_number: int, params: DownloadParams):
    '''
    Downloads the metadata and embedding file for a shard, joins them together, and writes a parquet table
    '''
    table_path = get_shard_dest_path(shard_number, params)

    try:
        return pq.read_metadata(table_path).num_rows
    except:
        num_rows = download_shard(shard_number, params)

        # The laion-5b dataset stores embeddings and metadata as separate files.  We combine the two into a
        # single parquet file by making the embedding a fixed size list array
        embedding_arr = load_embedding(shard_number, params)
        metadata_table = load_metadata(shard_number, params)
        metadata_table = metadata_table.append_column(pa.field("vector", embedding_arr.type), embedding_arr)

        pq.write_table(metadata_table, table_path)
    
    cleanup_temp_files(shard_number, params)
    
    return num_rows

def download_nrows(num_rows: int, params: DownloadParams):
    '''
    Downloads `nrows` of the laion-5b dataset from the internet
    '''
    print(f"Downloading {num_rows} rows of data")
    remaining = num_rows
    shard_number = 0
    while remaining > 0:
        remaining -= download_and_convert_shard(shard_number, params)
        progress = 100 * (1 - (remaining / num_rows))
        print(f"{remaining} rows remaining ({progress:02.2f}%)")
        shard_number = shard_number + 1

def initialize_directories(params: DownloadParams):
    '''
    Create working/destination directories, if they don't already exist
    '''
    os.makedirs(params.dest_parquet_dataset, exist_ok=True)
    os.makedirs(params.working_directory, exist_ok=True)

if __name__ == "__main__":
    params = DownloadParams(working_directory=pathlib.Path("/tmp/download"), dest_parquet_dataset=pathlib.Path("/home/pace/dev/data/laion_100m"), target_num_rows=100000000)
    initialize_directories(params)
    download_nrows(5_000_000, params)
