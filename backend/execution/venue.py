
import logging
import yaml
from multiprocessing import Process, Manager
from backend.execution.book import run_book_worker

__kafka_conf: str = "/app/backend/config/kafka.yml"

class Venue:
    def __init__(self, name: str | None = None, config_path: str|None=None, logger: logging.Logger | None = None):
        self.config_path: str | None = config_path or None
        self.config: dict = {}
        self.logger: logging.Logger | None = logger
        self.name: str = name or "Generic"
        self.symbols: list[str] = []
        self.manager = Manager()
        self.shared_bbo = self.manager.dict()
        self.processes: dict[str, Process] = {}
    
    def __enter__(self):
        if self.config_path is not None:
            try:
                with open(self.config_path, "r") as f:
                    self.config = yaml.safe_load(f)
                    self.symbols = self.config["valid_symbols"]
                    self.__spin_up_relevant_books()
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"YAML file error for {self.name}, {e}")
        else:
            if self.logger:
                self.logger.warning(f"No valid config path for venue {self.name}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type and self.logger:
            self.logger.error(f"Issue with venue {self.name}: {exc_type}, {exc_value}, {traceback}. Dumping config")
        if self.config_path:
            try:
                with open(self.config_path, "w") as wf:
                    yaml.dump(self.config, wf)
            except Exception:
                if self.logger:
                    self.logger.error(f"No valid writable YAML for venue {self.name}")
        else:
            if self.logger:
                self.logger.warning(f"No valid config path for venue {self.name}")
        if self.books:
            self.__kill_books(list(self.books.keys()))


    def __spin_up_relevant_books(self):
        for sym in self.symbols:
            p = Process(target=run_book_worker, args=(sym, __kafka_conf, self.shared_bbo))
            p.start()
            self.processes[sym] = p

    def __kill_books(self, remove: list[str]) -> None:
        for k in remove:
            if k in self.processes:
                self.processes[k].terminate()
                self.processes[k].join()
                del self.processes[k]

    def generate_child_orders(self, symbol, price, size, partial_fill):
        """
        Checks if order is on or off market, then samples 
        """
        # if self.books:
            # if symbol in self.symbols and symbol in self.books:
                # on_market = None #TODO check if order is on market by reading book bbo
                #TODO call router
                #record success rate
                # pass