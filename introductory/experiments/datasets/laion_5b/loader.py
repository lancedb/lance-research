import os
import pathlib
import urllib.request
import argparse

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from dataclasses import dataclass

from images import ImageProcessor


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
    """
    The command-line parameters
    """

    working_directory: pathlib.Path
    dest_parquet_dataset: pathlib.Path
    target_num_rows: int
    download_images: bool


def get_metadata_url(shard_number: int):
    """URL of the metadata file"""
    return f"https://the-eye.eu/public/AI/cah/laion5b/embeddings/laion2B-en/laion2B-en-metadata/metadata_{shard_number:04}.parquet"


def get_embedding_url(shard_number: int):
    """URL of the embedding file"""
    return f"https://the-eye.eu/public/AI/cah/laion5b/embeddings/laion2B-en/img_emb/img_emb_{shard_number:04}.npy"


def get_embedding_working_path(shard_number: int, params: DownloadParams):
    """Temporary path for downloading the embedding"""
    return params.working_directory / f"embedding_{shard_number:04}.npy"


def get_metadata_working_path(shard_number: int, params: DownloadParams):
    """Temporary path for downloading the metadata"""
    return params.working_directory / f"meta_{shard_number:04}.parquet"


def get_images_working_path(shard_number: int, params: DownloadParams):
    """Temporary path for downloading the images"""
    return params.working_directory / f"images_{shard_number:04}"


def get_shard_dest_path(shard_number: int, params: DownloadParams):
    """The path of a shard in our parquet dataset"""
    return params.dest_parquet_dataset / f"shard_{shard_number:04}.parquet"


def download_file_if_needed(src: pathlib.Path, dest: pathlib.Path, title: str):
    """Downloads a file from the internet, if it does not already exist"""
    if os.path.exists(dest):
        return
    # We download to .partial first and then rename.  This way, if the script is interrupted
    # during the download, we won't assume the partially downloaded file is complete
    partial_dest = dest.parent / (dest.name + ".partial")
    bar_format = f"{title}: " + "{l_bar}{bar}{r_bar}"
    with TqdmUpTo(
        unit="B", unit_scale=True, mininterval=5.0, bar_format=bar_format
    ) as progress:
        urllib.request.urlretrieve(src, partial_dest, progress.update_to)
    os.replace(partial_dest, dest)


def download_shard(shard_number: int, params: DownloadParams):
    """Downloads a shard of data from the internet into our working directory"""
    metadata_url = get_metadata_url(shard_number)
    metadata_dest = get_metadata_working_path(shard_number, params)
    download_file_if_needed(
        metadata_url, metadata_dest, f"Shard {shard_number} metadata "
    )

    embedding_url = get_embedding_url(shard_number)
    embedding_dest = get_embedding_working_path(shard_number, params)
    download_file_if_needed(
        embedding_url, embedding_dest, f"Shard {shard_number} embedding"
    )

    return pq.read_metadata(metadata_dest).num_rows


def load_embedding(shard_number: int, params: DownloadParams):
    """
    Load an embedding file from our working directory as a pyarrow array
    """
    embedding_path = get_embedding_working_path(shard_number, params)
    embedding_arr = np.load(embedding_path)
    # The laion-5B dataset uses float16 for embeddings.  Parquet does not support this and
    # so we convert to float32
    # Can comment this out if using PyArrow 15.0.0 (or nightly right as of this comment)
    # embedding_arr = embedding_arr.astype(np.single)
    width = embedding_arr.shape[1]
    embedding_arr.shape = -1
    return pa.FixedSizeListArray.from_arrays(embedding_arr, width)


def load_metadata(shard_number: int, params: DownloadParams):
    """
    Load a metadata file from our working directory as a pyarrow table
    """
    metadata_path = get_metadata_working_path(shard_number, params)
    return pq.read_table(metadata_path)


def cleanup_temp_files(shard_number: int, params: DownloadParams):
    """
    Remove the files from our working directory to free up space once they are joined
    """

    def try_delete(path):
        try:
            os.remove(path)
        except OSError:
            pass

    try_delete(get_embedding_working_path(shard_number, params))
    try_delete(get_metadata_working_path(shard_number, params))


def download_and_convert_shard(
    shard_number: int, remaining: int, params: DownloadParams
):
    """
    Downloads the metadata and embedding file for a shard, joins them together, and writes a parquet table

    Will output up to `remaining` rows. This remaining limit also limits the number
    of rows that are fetched. However, the full metadata and vectors will be fetched
    and then truncated.

    Will also download images if `params.download_images` is True
    """
    # If we are downloading images, will write metadata + vector to tmp path,
    # then write to final path when adding images.
    dest_path = get_shard_dest_path(shard_number, params)

    try:
        return pq.read_metadata(dest_path).num_rows
    except:
        num_rows = download_shard(shard_number, params)
        num_rows = min(num_rows, remaining)

        # The laion-5b dataset stores embeddings and metadata as separate files.  We combine the two into a
        # single parquet file by making the embedding a fixed size list array
        embedding_arr = load_embedding(shard_number, params)
        metadata_table = load_metadata(shard_number, params)
        metadata_table = metadata_table.append_column(
            pa.field("vector", embedding_arr.type), embedding_arr
        )
        metadata_table = metadata_table.slice(0, num_rows)

        if params.download_images:
            processor = ImageProcessor(
                metadata_table["url"],
                dest_path,
            )

            processor.prepare()

            reader = processor.join_images(metadata_table.to_reader(max_chunksize=1024))
            schema = metadata_table.schema.append(
                pa.field("img", pa.binary(), nullable=True)
            )
            with pq.ParquetWriter(dest_path, schema) as writer:
                try:
                    for batch in reader:
                        writer.write_batch(batch)
                except:
                    writer.close()
                    os.remove(dest_path)
        else:
            # Can write metadata table directly to final path
            pq.write_table(metadata_table, dest_path)

    # cleanup_temp_files(shard_number, params)

    return num_rows


def download_nrows(params: DownloadParams):
    """
    Downloads `nrows` of the laion-5b dataset from the internet
    """
    print(f"Downloading {params.target_num_rows} rows of data")
    remaining = params.target_num_rows
    shard_number = 0
    while remaining > 0:
        remaining -= download_and_convert_shard(shard_number, remaining, params)
        progress = 100 * (1 - (remaining / params.target_num_rows))
        print(f"{remaining} rows remaining ({progress:02.2f}%)")
        shard_number = shard_number + 1


def initialize_directories(params: DownloadParams):
    """
    Create working/destination directories, if they don't already exist
    """
    os.makedirs(params.dest_parquet_dataset, exist_ok=True)
    os.makedirs(params.working_directory, exist_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Download a subset of the laion-5b dataset from the internet."""
    )
    parser.add_argument("num_rows", type=int, help="Minimum number of rows to download")
    parser.add_argument(
        "dest_path", type=pathlib.Path, help="Path to write the parquet dataset to"
    )
    parser.add_argument(
        "--working-directory",
        type=pathlib.Path,
        default=pathlib.Path("/tmp/download"),
        help="Directory to use for temporary files",
    )
    parser.add_argument(
        "--download-images", action="store_true", help="Whether to download images"
    )
    args = parser.parse_args()

    params = DownloadParams(
        working_directory=pathlib.Path(args.working_directory),
        dest_parquet_dataset=pathlib.Path(args.dest_path),
        target_num_rows=args.num_rows,
        download_images=args.download_images,
    )
    initialize_directories(params)
    download_nrows(params)
