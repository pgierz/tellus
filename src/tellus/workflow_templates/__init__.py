"""
Workflow templates package for Tellus Earth Science workflows.

This package provides pre-defined workflow templates for common Earth science
computational patterns, including data preprocessing, model execution,
and analysis workflows.
"""

from .earth_science_templates import (
    create_climate_data_preprocessing_template,
    create_esm_model_run_template,
    create_climate_analysis_template,
    get_all_earth_science_templates
)

__all__ = [
    "create_climate_data_preprocessing_template",
    "create_esm_model_run_template", 
    "create_climate_analysis_template",
    "get_all_earth_science_templates"
]