from pathlib import Path
import shutil
import sqlite3
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
from img2dataset import download
from tqdm import tqdm

class ImageProcessor:
    def __init__(
        self,
        urls,
        output_folder,
    ):
        self.output_folder = output_folder

        self.tmp_dir = Path(tempfile.mkdtemp())
        self.input_file = self.tmp_dir / "urls.parquet"
        pq.write_table(pa.Table.from_pydict({"url": urls}), self.input_file)

        self.image_index = None
    
    def __del__(self):
        shutil.rmtree(self.tmp_dir)

    def prepare(self):
        # Download the images into Parquet files. This doesn't necessarily preserve
        # order nor does it get all images (since some might be unavailable).
        self.download_images()
        # Create an index of the images that were downloaded.
        self.create_image_index()

    def join_images(self, reader):
        if self.image_index is None:
            raise RuntimeError("Image index not initialized. Call prepare() first.")

        for batch in reader:
            images = []
            for url in batch["url"]:
                try:
                    image = self.image_index[url.as_py()]
                except KeyError:
                    image = None
                images.append(image)
            tab = pa.Table.from_batches([batch])
            tab = tab.append_column(pa.field("img", pa.binary(), nullable=True), pa.array(images, pa.binary()))
            yield tab.to_batches()[0]
    
    def download_images(self):
        print("Downloading images")
        download(
            # distributor="multiprocessing",
            processes_count=8,
            thread_count=128,
            retries=1,
            # Transform params
            resize_only_if_bigger=True,
            image_size=256,
            # Input params
            url_list=str(self.input_file),
            input_format="parquet",
            url_col="url",
            # Output params
            output_folder=str(self.tmp_dir / "parquet"),
            output_format="parquet",
            number_sample_per_shard=10_000,
        )

    def create_image_index(self):
        print("Building image index")
        self.image_index = ImageIndex(self.tmp_dir / "images.db")
        
        pq_files = list((self.tmp_dir / "parquet").glob("*.parquet"))
        for pq_file in tqdm(pq_files):
            with pq.ParquetFile(pq_file) as reader:
                for batch in reader.iter_batches():
                    for url, image in zip(batch["url"], batch["jpg"]):
                        self.image_index[url.as_py()] = image.as_py()

            # Once we are done with a file, delete it to free up space.
            pq_file.unlink()


class ImageIndex:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)

        cur = self.conn.cursor()
        cur.execute("""
        create table if not exists images(
            url text primary key,
            image blob default NULL
        );
        """)
        self.conn.commit()
    
    def __setitem__(self, url, image):
        cur = self.conn.cursor()
        cur.execute("insert or replace into images(url, image) values (?, ?)", (url, image))
        self.conn.commit()

    def __getitem__(self, url):
        cur = self.conn.cursor()
        cur.execute("select image from images where url = ?", (url,))
        image = cur.fetchone()
        if image is None:
            raise KeyError(url)
        return image[0]

    def __len__(self):
        cur = self.conn.cursor()
        cur.execute("select count(*) from images")
        return cur.fetchone()[0]
    