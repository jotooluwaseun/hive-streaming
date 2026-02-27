from pipeline.src.validation_bronze import run as validation_run
import logging

class ValidationJob:
    name = "validation"
    def run(self):
        logging.info("Running Bronze validation...")
        validation_run()
        logging.info("Bronze validation complete.")