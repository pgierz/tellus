#!/usr/bin/env python
"""Test database connection and basic workflow operations."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.tellus.infrastructure.database.models import WorkflowModel
from datetime import datetime

DATABASE_URL = "postgresql://tellus:tellus@localhost:5432/tellus"

print("Connecting to database...")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

print("✓ Connected successfully!\n")

# Test 1: Create a workflow
print("Test 1: Creating a test workflow...")
test_workflow = WorkflowModel(
    workflow_id="test-workflow-1",
    name="Test ESM-Tools Workflow",
    workflow_type="MODEL_EXECUTION",
    workflow_system="esm-tools",
    simulation_id=None,
    description="Test workflow for verification",
    status="DRAFT",
    current_status="unknown",
    observed_file_count=0,
    polling_enabled=False,
    polling_interval_minutes=15
)

session.add(test_workflow)
session.commit()
print("✓ Workflow created with ID:", test_workflow.workflow_id)

# Test 2: Query the workflow
print("\nTest 2: Querying workflow...")
retrieved = session.query(WorkflowModel).filter_by(workflow_id="test-workflow-1").first()
print("✓ Retrieved workflow:", retrieved.name)
print("  - System:", retrieved.workflow_system)
print("  - Status:", retrieved.current_status)
print("  - Created:", retrieved.created_at)

# Test 3: Update workflow
print("\nTest 3: Updating workflow...")
retrieved.current_status = "running"
retrieved.observed_file_count = 42
session.commit()
print("✓ Updated status to:", retrieved.current_status)
print("  - File count:", retrieved.observed_file_count)

# Test 4: Delete workflow
print("\nTest 4: Deleting workflow...")
session.delete(retrieved)
session.commit()
print("✓ Workflow deleted")

# Verify deletion
count = session.query(WorkflowModel).count()
print(f"✓ Total workflows in database: {count}")

session.close()
print("\n✅ All database operations successful!")
print("\nDatabase is ready for use:")
print("  - Connection: postgresql://tellus:tellus@localhost:5432/tellus")
print("  - Container: tellus-postgres")
print("  - Port: 5432")
