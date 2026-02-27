import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from pipeline.orchestration.utils.job_runner import JobRunner
from pipeline.orchestration.jobs.bronze_job import BronzeJob
from pipeline.orchestration.jobs.validation_job import ValidationJob
from pipeline.orchestration.jobs.silver_job import SilverJob
from pipeline.orchestration.jobs.gold_job import GoldJob


def run_job(job):
    runner = JobRunner(job)
    runner.run()
    return job.name


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    logging.info("Starting Hive Streaming Orchestrator")

    jobs = [
        BronzeJob(),
        ValidationJob(),
        SilverJob(),
        GoldJob()
    ]

    # Run all jobs concurrently
    with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        futures = {executor.submit(run_job, job): job for job in jobs}

        for future in as_completed(futures):
            job_name = futures[future].name
            try:
                future.result()
                logging.info(f"{job_name} completed successfully")
            except Exception as e:
                logging.error(f"{job_name} failed: {e}")

    logging.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()
