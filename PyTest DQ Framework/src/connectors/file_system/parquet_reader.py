import os
import pandas as pd


class ParquetReader:
    """
    A utility class to read Parquet files from a given path, supporting both single files and directories with partitioned subfolders.
    """

    def process(self, path: str) -> pd.DataFrame:
        """
        Read Parquet file(s) from the given path. If the path is a directory, it will recursively read all Parquet files,
        infer partition columns from folder names (e.g., 'partition_date=2000-01'), and concatenate the data.

        Args:
            path (str): Path to the Parquet file or directory.

        Returns:
            pd.DataFrame: DataFrame containing the file's or directory's data.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Path does not exist: {path}")

        dfs = []

        if os.path.isfile(path):
            try:
                df = pd.read_parquet(path)
                dfs.append(df)
            except Exception as e:
                raise RuntimeError(f"Failed to read Parquet file {path}: {e}")
        elif os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for file in files:
                    if file.endswith(('.parquet', '.pq')):
                        file_path = os.path.join(root, file)
                        try:
                            df = pd.read_parquet(file_path)
                            # Infer partition columns from the relative path
                            rel_path = os.path.relpath(root, path)
                            if rel_path != '.':
                                partitions = rel_path.split(os.sep)
                                for part in partitions:
                                    if '=' in part:
                                        key, value = part.split('=', 1)
                                        df[key] = value
                            dfs.append(df)
                        except Exception as e:
                            raise RuntimeError(f"Failed to read Parquet file {file_path}: {e}")
            if not dfs:
                raise RuntimeError(f"No Parquet files found in directory: {path}")
        else:
            raise ValueError(f"Path is neither a file nor a directory: {path}")

        return pd.concat(dfs, ignore_index=True)