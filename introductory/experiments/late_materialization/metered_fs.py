import io

import pyarrow as pa
import pyarrow.fs as pa_fs


class MeteredFSHandler(pa_fs.FileSystemHandler):
    def __init__(self, fs):
        self.fs = fs
        self.num_ios = 0
        self.total_bytes = 0

    def open_input_stream(self, path):
        f = self.fs.open_input_stream(path)
        return pa.PythonFile(MeteredInputFile(self, f), mode="rb")

    def open_input_file(self, path):
        f = self.fs.open_input_file(path)
        return pa.PythonFile(MeteredInputFile(self, f), mode="rb")

    def copy_file(self, src, dest):
        return self.fs.copy_file(src, dest)

    def move_file(self, src, dest):
        return self.fs.move_file(src, dest)

    def move(self, src, dest):
        return self.fs.move(src, dest)

    def delete_file(self, path):
        return self.fs.delete_file(path)

    def create_dir(self, path, recursive):
        return self.fs.create_dir(path, recursive)

    def delete_dir(self, path):
        return self.fs.delete_dir(path)

    def delete_dir_contents(self, path):
        return self.fs.delete_dir_contents(path)

    def delete_root_dir_contents(self):
        return self.fs.delete_root_dir_contents()

    def get_file_info(self, path, **kwargs):
        return self.fs.get_file_info(path, **kwargs)

    def get_file_info_selector(self, selector, **kwargs):
        return self.fs.get_file_info_selector(selector, **kwargs)

    def get_type_name(self):
        return self.fs.get_type_name()

    def normalize_path(self, path):
        return self.fs.normalize_path(path)

    def open_append_stream(self, path):
        return self.fs.open_append_stream(path)

    def open_output_stream(self, path):
        return self.fs.open_output_stream(path)


class MeteredInputFile:
    def __init__(self, fs, f):
        self.fs = fs
        self.f = f

    def read_buffer(self, n=-1):
        res = self.f.read_buffer(n)
        self.fs.num_ios += 1
        self.fs.total_bytes += len(res)
        return res

    # Delegate all other methods to the underlying filesystem
    def __getattr__(self, attr):
        # For debugging, can see which methods are being called
        # print(attr)
        return getattr(self.f, attr)
