import logging
import time

class JobRunner:
    def __init__(self, job, retries=3, backoff=5):
        self.job = job
        self.retries = retries
        self.backoff = backoff

    def run(self):
        attempt = 1
        while attempt <= self.retries:
            try:
                logging.info(f"▶️ Starting job: {self.job.name} (attempt {attempt})")
                self.job.run()
                logging.info(f"✅ Job succeeded: {self.job.name}")
                return
            except Exception as e:
                logging.error(f"❌ Job failed: {self.job.name} — {e}")
                if attempt == self.retries:
                    logging.error(f"💥 Job {self.job.name} failed after {self.retries} attempts")
                    raise
                logging.info(f"⏳ Retrying in {self.backoff} seconds...")
                time.sleep(self.backoff)
                attempt += 1
