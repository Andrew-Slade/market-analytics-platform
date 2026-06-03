import pytest
import subprocess
import logging
from ingestion.subscription_service import Subscribe

class Test_Subscription_Service:
    
    @pytest.fixture(autouse=True)
    def setup(self):
        logger = logging.getLogger("test_logger")
        self.s = Subscribe(logger, "tests/test_data.yml")

    
    def test_init(self):
        assert self.s is not None
    
    def test_get_subscriptions(self):
        self.s._Subscribe__get_subscriptions()
        assert self.s.config == ["BTC-USD","SPY","SOL-USD","QQQ","IWM"]

    def test_kill_subscription(self):
        self.s.active_subscriptions["BTC-USD"] = subprocess.Popen(["python", "-c", "import time\nwhile True: print('looping...'); time.sleep(1)"])
        self.s._Subscribe__kill_subscription(["BTC-USD"])
        assert self.s.active_subscriptions == {}