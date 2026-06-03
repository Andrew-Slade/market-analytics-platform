import logging
import yaml
from datetime import datetime
import argparse
from collections import defaultdict
import subprocess
import time
from datetime import timedelta



__log_location = f"logs/subscriptions_{datetime.now().strftime("%Y-%m-%d")}.log"
__config_location = "config/logging.yml"
__subscription_config_file = "config/subscription.yml"

class Subscribe:
    def __init__(self, logger: logging.Logger, config: str):
        self.logger = logger
        self.config_location = config
        self.__get_subscriptions()
        self.active_subscriptions: dict = defaultdict()
        self.polling_period = 300

    def __get_subscriptions(self) -> None:
        try:
            with open(self.config_location) as f:
                self.config: dict = yaml.safe_load(f)["tickers"]
                if len(self.config)   == 0:
                    raise Exception("Not enough arguments")
        except FileNotFoundError:
            t = f"Config file for subscriptions not found at {self.config_location}"
            logger.error(t)
            raise FileNotFoundError(t)
        except yaml.YAMLError:
            t = f"Issue with yaml formatting at {self.config_location}"
            logger.error(t)
            raise yaml.YAMLError(t)
        except Exception:
            t = f"Unknown error with subscription configuration {self.config_location}"
            logger.error(t)
            raise Exception(t)

    def __del__(self):
        self.__kill_subscription(list(self.active_subscriptions.keys()))

    def run(self):
        last = datetime.now() - timedelta(seconds = self.polling_period)
        current_subscription_count = 0
        new_subscription_count = 0
        first_run = True
        while True:
            time.sleep(0.1)
            now = datetime.now()
            current_subscription_count = len(self.active_subscriptions) #poll subscription count
            if (now - last).total_seconds() > self.polling_period or first_run:
                logging.debug(f"{self.polling_period} seconds up, checking for new subscriptions")
                self.__get_subscriptions()
                new_subscription_count = len(self.config) #when we load new ones from disk, this updates
                last = datetime.now()
                if first_run:
                    first_run = False
            diff = current_subscription_count - new_subscription_count
            adiff = abs(diff)
            diff = diff / adiff if adiff != 0 else 0
            match diff:
                case -1: #more new subscriptions than current
                    self.logger.debug("New subscriptions detected, spinning up subscriptions...")
                    self.__spin_up_subscription(self.config)
                case 1: #more current subscriptions than new
                    self.logger.debug("Fewer subscriptions detected, reducing...")
                    to_remove = [ i for i in self.active_subscriptions if i not in self.config]
                    self.__kill_subscription(to_remove)
                case _: #no change to size
                    #we still need to check all the subscriptions are correct
                    if set(self.active_subscriptions.keys()) != set(self.config): #only act on list changes
                        to_remove = [ i for i in self.active_subscriptions if i not in self.config]
                        to_add = [ i for i in self.config if i not in self.active_subscriptions]
                        #two ifs, not if else, as we probably could see both
                        if len(to_add) > 0:
                            self.__spin_up_subscription(to_add)
                        if len(to_remove) > 0:
                            self.__kill_subscription(to_remove)
            
    def __spin_up_subscription(self, add: list[str]):
        for i in add:
            if i not in self.active_subscriptions:
                self.active_subscriptions[i] = subprocess.Popen(["python", "ingestion/generic.py", i])
    
    def __kill_subscription(self, remove: list[str]):
        for i in remove:
            try:
                self.active_subscriptions[i].terminate()
                self.active_subscriptions[i].wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.logger.warning(f"Process {i} refusing to close, force killing")
                self.active_subscriptions[i].kill()
                self.active_subscriptions[i].wait()
            del self.active_subscriptions[i]
            
def setup_logging(verbose: bool) -> logging.Logger:
    try:
        print("Reading config")
        with open(__config_location) as lfile:
            log_conf = yaml.safe_load(lfile)
            print(f"Initialized config: {log_conf} from \n{__config_location}")

        logger = logging.getLogger(__name__)
        file_handler = logging.FileHandler(__log_location)
        logging.basicConfig(level=logging.INFO if not verbose else logging.DEBUG)
        logger.addHandler(file_handler)
        logger.info(f"Set logging destination: {__log_location}, logging mode {logging.getLevelName(logger.getEffectiveLevel())}")
    except Exception:
        print("Logging file format corrupted")
        exit(1)
    else:
        return logger

def arg_parser() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("-v", "--verbose", help="Enable debugging mode", action="store_true")
    return p.parse_args()

if __name__=="__main__":
    args: argparse.Namespace = arg_parser()
    logger = setup_logging(args.verbose)
    try:
        s : Subscribe = Subscribe(logger, __subscription_config_file)
    except Exception as e:
        logger.error(f"Failure to start subscription service, error: {e}")
        exit(1)
    s.run()