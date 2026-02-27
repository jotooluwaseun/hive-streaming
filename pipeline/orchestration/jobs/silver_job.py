from pipeline.src.silver_transform import run as silver_run
import logging

class SilverJob:
    name = "silver"
    def run(self):
        logging.info("Running Silver transfomation...")
        silver_run()
        logging.info("Silver transfomation complete.")