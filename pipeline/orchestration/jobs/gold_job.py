from pipeline.src.gold_metrics import run as gold_run
import logging

class GoldJob:
    name = "gold"
    def run(self):
        logging.info("Running Gold metrics...")
        gold_run()
        logging.info("Gold metrics complete.")