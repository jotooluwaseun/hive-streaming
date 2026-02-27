"""
Paths Configuration Test Suite

This module validates the directory and path structure used across the
entire Medallion pipeline. It ensures that all data and checkpoint
locations are correctly created and available before any pipeline
layer executes.

The suite covers:

1. Core Data Directories
   - Verifies that all primary storage locations (Raw, Bronze,
     Bronze-Validated, Silver, Gold, DQ results) exist on disk.

2. Checkpoint Directories
   - Confirms that each streaming pipeline stage has a corresponding
     checkpoint directory to guarantee exactly-once processing and
     recovery.

3. Gold Output Directories
   - Ensures that viewer-level and content-level Gold metric tables
     have valid output locations.

These tests guarantee that the filesystem layout is stable, consistent,
and ready for all ingestion, validation, transformation, and metrics jobs.
"""


from pipeline.src.paths import ( 
    DATA_DIR,
    source_raw_path,
    bronze_path,
    bronze_validated_path,
    silver_path,
    gold_path,
    dq_results_path,
    checkpoint_source_to_bronze_path,
    checkpoint_bronze_to_bronze_validated_path,
    checkpoint_bronze_to_silver_path,
    checkpoint_silver_to_gold_path,
    checkpoint_gold_viewer_path,
    checkpoint_gold_content_path,
    gold_viewer_path,
    gold_content_path,
)

def test_data_directories_exist():
    assert DATA_DIR.exists()
    assert source_raw_path.exists()
    assert bronze_path.exists()
    assert bronze_validated_path.exists()
    assert silver_path.exists()
    assert gold_path.exists()
    assert dq_results_path.exists()

def test_checkpoint_directories_exist():
    assert checkpoint_source_to_bronze_path.exists()
    assert checkpoint_bronze_to_bronze_validated_path.exists()
    assert checkpoint_bronze_to_silver_path.exists()
    assert checkpoint_silver_to_gold_path.exists()
    assert checkpoint_gold_viewer_path.exists()
    assert checkpoint_gold_content_path.exists()

def test_gold_output_directories_exist():
    assert gold_viewer_path.exists()
    assert gold_content_path.exists()
