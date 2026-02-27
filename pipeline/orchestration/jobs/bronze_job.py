from pipeline.src.ingestion_raw import run as bronze_run
import logging

class BronzeJob:
    name = "bronze"
    def run(self):
        logging.info("Running Bronze ingestion...")
        bronze_run()
        logging.info("Bronze ingestion complete.")
