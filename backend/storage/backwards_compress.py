"""
Manages fragmented parquet files
Compressess them into a single parquet file by symbole/day
Helps keep the file system clean and makes it easier to read the data back in
"""
from datetime import datetime
import logging
import shutil
import time
from typing import Generator, Tuple

import duckdb
from pathlib import Path
import deltalake
import pyarrow as pa
import os

class BackCompress:
    def __init__(self):
        BASE = Path.cwd()
        self.data_path = BASE / "data" / "**" / "*.snappy.parquet"
        self.con = duckdb.connect()
        self.con.execute("INSTALL delta; LOAD delta;")
        self.query = f"""
            SELECT 
                *
            FROM read_parquet(?, hive_partitioning=true)
        """
        self.base = BASE / "data"

    def walk_data_dir(self, logger: logging.Logger) -> Generator[Tuple[str, str, str, str], None, None]:
        """
        Generator walks the data dir and yields a tuple of valid dates
        """
        logger.debug(f"Walking {self.base}") if logger else None
        for year_dir in self.base.iterdir():
            for month_dir in year_dir.iterdir():
                for day_dir in month_dir.iterdir():
                    for symbol_dir in day_dir.iterdir():
                        logger.debug(f"Walking {year_dir.name}/{month_dir.name}/{day_dir.name}/{symbol_dir.name}") if logger else None
                        if len(list(symbol_dir.glob("*.snappy.parquet"))) > 1:
                            logger.debug(f"Found {len(list(symbol_dir.glob('*.snappy.parquet')))} files in {year_dir.name}/{month_dir.name}/{day_dir.name}/{symbol_dir.name}") if logger else None
                            yield year_dir.name.split("=")[1], month_dir.name.split("=")[1], day_dir.name.split("=")[1], symbol_dir.name

    def write(self, arrow_table: pa.Table, path:str, del_path: str, logger: logging.Logger) -> None:
        """
        Removes all fragmented parquet files and writes a single compressed parquet file
        """
        for f in Path(del_path).absolute().iterdir():
            if f.is_file() and f.suffix == ".parquet":
                os.remove(f)
                logger.info(f"Deleted {f}") if logger else None
            else:
                shutil.rmtree(f)
                logger.debug(f"Deleted folder {f}") if logger else None
        logger.info(f"Writing {path}")
        deltalake.write_deltalake(path, arrow_table)
        logger.info(f"Written {path}")

    def read(self, path: str, logger: logging.Logger):
        """
        Reads all fragmented parquet files and returns a single arrow table
        """
        logger.debug(f"Reading {path}") if logger else None
        arrow_table = self.con.execute(self.query, [path]).to_arrow_table()
        return arrow_table
        
def setup_file_logger(name: str, logfile: str, verbose: bool = False) -> logging.Logger:
    Path(logfile).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(logfile),
            logging.StreamHandler()
        ],
        force=True
    )

    return logging.getLogger(name)

if __name__ == "__main__":
    while True:
        logger: logging.Logger = setup_file_logger(
            f"backward_compress",
            f"/app/backend/logs/backward_compress_{datetime.now():%Y-%m-%d}.log",
            verbose=True
        )
        logger.info("Starting backward compression")
        bk = BackCompress()
        for year, month, day, symbol in bk.walk_data_dir(logger):
            logger.debug(f"Checking {year}/{month}/{day}/{symbol}") if logger else None
            if int(month) != int(datetime.now().strftime("%M")) and int(day) != int(datetime.now().strftime("%d")):
                logger.info(f"Compressing {year}/{month}/{day}/{symbol}")
                table = bk.read(path=f"/app/data/year={year}/month={month}/day={day}/{symbol}/**/*.snappy.parquet", logger=logger)
                bk.write(table, f"/app/data/year={year}/month={month}/day={day}/{symbol}/compressed.snappy.parquet", f"/app/data/year={year}/month={month}/day={day}/{symbol}", logger)
            else:
                logger.info(f"Skipping {year}/{month}/{day}/{symbol} because it is the current day")
        time.sleep(60*60*6) #sleep half a day: 60 seconds, 60 min, 6 hours