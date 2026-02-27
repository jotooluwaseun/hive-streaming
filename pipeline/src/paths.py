"""
Paths Configuration Module

This module defines the single source of truth for all storage locations.
It centralizes all filesystem paths used across Raw, Bronze, Silver, and 
Gold layers, as well as checkpoints and data-quality outputs.

source_raw_path = "../data/source_raw"
bronze_path = "../data/bronze"
silver_path = "../data/silver"
gold_folder_path = "../data/gold"

checkpoint_source_to_bronze_path = "../data/checkpoints/source_to_bronze"
checkpoint_bronze_to_silver_path = "../data/checkpoints/bronze_to_silver"
checkpoint_silver_to_gold_path = "../data/checkpoints/silver_to_gold"
"""

from pathlib import Path

# Auto-detect project root (the folder containing pipeline)
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]  

DATA_DIR = PROJECT_ROOT / "pipeline" / "data"

# Data layer paths
source_raw_path = DATA_DIR / "source_raw"
bronze_path = DATA_DIR / "bronze"
bronze_validated_path = DATA_DIR / "bronze_validated"
silver_path = DATA_DIR / "silver"
gold_path = DATA_DIR / "gold"

# Checkpoint paths
checkpoint_source_to_bronze_path = DATA_DIR / "checkpoints" / "source_to_bronze"
checkpoint_bronze_to_bronze_validated_path = DATA_DIR / "checkpoints" / "bronze_to_bronze_validated"
checkpoint_bronze_to_silver_path = DATA_DIR / "checkpoints" / "bronze_to_silver"
checkpoint_silver_to_gold_path = DATA_DIR / "checkpoints" / "silver_to_gold"
checkpoint_gold_viewer_path = DATA_DIR / "checkpoints" / "gold_viewer"
checkpoint_gold_content_path = DATA_DIR / "checkpoints" / "gold_content"

# Data quality results path
dq_results_path = silver_path.parent / "dq_results"

# Viewer and Conrent Quality of Service Paths
gold_viewer_path = gold_path / "viewer_qos"
gold_content_path = gold_path / "content_qos"

for p in [
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
]:
    p.mkdir(parents=True, exist_ok=True)
