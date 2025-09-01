"""
Tests for WorkflowEntity domain entity.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

# Add src directory to path to avoid the scoutfs import issue
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "src"))

from tellus.domain.entities.workflow import (ResourceRequirement,
                                             WorkflowEntity, WorkflowStatus,
                                             WorkflowStep, WorkflowType,
                                             WorkflowValidationError)


class TestWorkflowEntity:
    """Test suite for WorkflowEntity."""
    
    def test_create_basic_workflow(self):
        """Test creating a basic workflow."""
        step1 = WorkflowStep(
            step_id="preprocess",
            name="Preprocess data",
            command="python preprocess.py",
            dependencies=[]
        )
        
        workflow = WorkflowEntity(
            workflow_id="test-workflow-001",
            name="Test Workflow",
            description="A simple test workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[step1]
        )
        
        assert workflow.workflow_id == "test-workflow-001"
        assert workflow.name == "Test Workflow"
        assert workflow.workflow_type == WorkflowType.DATA_PREPROCESSING
        assert workflow.status == WorkflowStatus.DRAFT
        assert len(workflow.steps) == 1
        assert workflow.steps[0].step_id == "preprocess"
    
    def test_create_workflow_with_multiple_steps_and_dependencies(self):
        """Test creating a workflow with dependencies between steps."""
        step1 = WorkflowStep(
            step_id="download",
            name="Download data",
            command="python download.py",
            dependencies=[]
        )
        
        step2 = WorkflowStep(
            step_id="preprocess",
            name="Preprocess data", 
            command="python preprocess.py",
            dependencies=["download"]
        )
        
        step3 = WorkflowStep(
            step_id="analyze",
            name="Analyze results",
            command="python analyze.py",
            dependencies=["preprocess"]
        )
        
        workflow = WorkflowEntity(
            workflow_id="complex-workflow",
            name="Complex Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION,
            steps=[step1, step2, step3]
        )
        
        assert len(workflow.steps) == 3
        assert workflow.get_step("preprocess").dependencies == ["download"]
        assert workflow.get_step("analyze").dependencies == ["preprocess"]
    
    def test_workflow_validation_success(self):
        """Test successful workflow validation."""
        step1 = WorkflowStep(
            step_id="step1",
            name="Step 1",
            command="echo 'step1'",
            dependencies=[]
        )
        
        step2 = WorkflowStep(
            step_id="step2", 
            name="Step 2",
            command="echo 'step2'",
            dependencies=["step1"]
        )
        
        workflow = WorkflowEntity(
            workflow_id="valid-workflow",
            name="Valid Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[step1, step2]
        )
        
        # Should not raise any exception
        errors = workflow.validate()
        assert errors == []
    
    def test_workflow_validation_fails_with_missing_dependency(self):
        """Test workflow validation fails when step references non-existent dependency."""
        step1 = WorkflowStep(
            step_id="step1",
            name="Step 1", 
            command="echo 'step1'",
            dependencies=["nonexistent"]  # This dependency doesn't exist
        )
        
        workflow = WorkflowEntity(
            workflow_id="invalid-workflow",
            name="Invalid Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[step1]
        )
        
        errors = workflow.validate()
        assert len(errors) > 0
        assert "nonexistent" in errors[0]
        assert "unknown dependency" in errors[0]
    
    def test_workflow_validation_fails_with_circular_dependency(self):
        """Test workflow validation detects circular dependencies."""
        step1 = WorkflowStep(
            step_id="step1",
            name="Step 1",
            command="echo 'step1'",
            dependencies=["step2"]
        )
        
        step2 = WorkflowStep(
            step_id="step2",
            name="Step 2", 
            command="echo 'step2'",
            dependencies=["step1"]
        )
        
        workflow = WorkflowEntity(
            workflow_id="circular-workflow",
            name="Circular Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[step1, step2]
        )
        
        errors = workflow.validate()
        assert len(errors) > 0
        assert "circular dependency" in errors[0].lower()
    
    def test_workflow_validation_fails_with_duplicate_step_ids(self):
        """Test workflow validation fails with duplicate step IDs."""
        step1 = WorkflowStep(
            step_id="duplicate",
            name="Step 1",
            command="echo 'step1'",
            dependencies=[]
        )
        
        step2 = WorkflowStep(
            step_id="duplicate",  # Duplicate ID
            name="Step 2",
            command="echo 'step2'", 
            dependencies=[]
        )
        
        workflow = WorkflowEntity(
            workflow_id="duplicate-workflow",
            name="Duplicate Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[step1, step2]
        )
        
        errors = workflow.validate()
        assert len(errors) > 0
        assert "duplicate" in errors[0].lower()
    
    def test_get_step_by_id(self):
        """Test getting a step by ID."""
        step1 = WorkflowStep(
            step_id="preprocess",
            name="Preprocess data",
            command="python preprocess.py",
            dependencies=[]
        )
        
        workflow = WorkflowEntity(
            workflow_id="test-workflow",
            name="Test Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[step1]
        )
        
        # Existing step
        found_step = workflow.get_step("preprocess")
        assert found_step is not None
        assert found_step.step_id == "preprocess"
        
        # Non-existent step
        missing_step = workflow.get_step("nonexistent")
        assert missing_step is None
    
    def test_add_step_to_workflow(self):
        """Test adding a step to an existing workflow."""
        workflow = WorkflowEntity(
            workflow_id="test-workflow",
            name="Test Workflow", 
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[]
        )
        
        step = WorkflowStep(
            step_id="new-step",
            name="New Step",
            command="echo 'new'",
            dependencies=[]
        )
        
        workflow.add_step(step)
        
        assert len(workflow.steps) == 1
        assert workflow.get_step("new-step") is not None
    
    def test_add_duplicate_step_fails(self):
        """Test that adding a step with duplicate ID fails."""
        step1 = WorkflowStep(
            step_id="duplicate",
            name="First Step",
            command="echo '1'",
            dependencies=[]
        )
        
        workflow = WorkflowEntity(
            workflow_id="test-workflow",
            name="Test Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[step1]
        )
        
        step2 = WorkflowStep(
            step_id="duplicate",
            name="Second Step", 
            command="echo '2'",
            dependencies=[]
        )
        
        with pytest.raises(ValueError, match="already exists"):
            workflow.add_step(step2)
    
    def test_remove_step_from_workflow(self):
        """Test removing a step from workflow."""
        step1 = WorkflowStep(
            step_id="step1",
            name="Step 1",
            command="echo '1'",
            dependencies=[]
        )
        
        step2 = WorkflowStep(
            step_id="step2",
            name="Step 2",
            command="echo '2'", 
            dependencies=["step1"]
        )
        
        workflow = WorkflowEntity(
            workflow_id="test-workflow",
            name="Test Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[step1, step2]
        )
        
        # Remove step1
        result = workflow.remove_step("step1")
        assert result is True
        assert len(workflow.steps) == 1
        assert workflow.get_step("step1") is None
        assert workflow.get_step("step2") is not None
        
        # Try to remove non-existent step
        result = workflow.remove_step("nonexistent")
        assert result is False
    
    def test_get_root_steps(self):
        """Test getting steps with no dependencies."""
        step1 = WorkflowStep(
            step_id="root1",
            name="Root 1",
            command="echo 'root1'",
            dependencies=[]
        )
        
        step2 = WorkflowStep(
            step_id="root2", 
            name="Root 2",
            command="echo 'root2'",
            dependencies=[]
        )
        
        step3 = WorkflowStep(
            step_id="child",
            name="Child",
            command="echo 'child'",
            dependencies=["root1"]
        )
        
        workflow = WorkflowEntity(
            workflow_id="test-workflow",
            name="Test Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[step1, step2, step3]
        )
        
        root_steps = workflow.get_root_steps()
        assert len(root_steps) == 2
        root_ids = [step.step_id for step in root_steps]
        assert "root1" in root_ids
        assert "root2" in root_ids
        assert "child" not in root_ids
    
    def test_workflow_parameters_and_template_variables(self):
        """Test workflow parameters and template variable substitution."""
        workflow = WorkflowEntity(
            workflow_id="parameterized-workflow",
            name="Parameterized Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[],
            parameters={"input_file": "data.nc", "output_dir": "/results"}
        )
        
        assert workflow.parameters["input_file"] == "data.nc"
        assert workflow.parameters["output_dir"] == "/results"
        
        # Test template variable replacement
        template = "Process {input_file} and save to {output_dir}"
        result = workflow.resolve_template_variables(template)
        assert result == "Process data.nc and save to /results"
    
    def test_workflow_with_resource_requirements(self):
        """Test workflow with resource requirements."""
        requirements = ResourceRequirement(
            cpu_cores=4,
            memory_gb=16,
            disk_space_gb=100,
            gpu_count=1,
            estimated_runtime=timedelta(hours=2)
        )
        
        step = WorkflowStep(
            step_id="compute-intensive",
            name="Compute Intensive Step",
            command="python heavy_computation.py",
            dependencies=[],
            resource_requirements=requirements
        )
        
        workflow = WorkflowEntity(
            workflow_id="resource-workflow",
            name="Resource Intensive Workflow",
            workflow_type=WorkflowType.MODEL_EXECUTION,
            steps=[step]
        )
        
        step_requirements = workflow.get_step("compute-intensive").resource_requirements
        assert step_requirements.cpu_cores == 4
        assert step_requirements.memory_gb == 16
        assert step_requirements.gpu_count == 1
        assert step_requirements.estimated_runtime == timedelta(hours=2)
    
    def test_workflow_tags_and_metadata(self):
        """Test workflow tags and metadata."""
        workflow = WorkflowEntity(
            workflow_id="tagged-workflow",
            name="Tagged Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[],
            tags={"climate", "preprocessing", "netcdf"},
            metadata={"author": "test", "version": "1.0", "project": "climate-study"}
        )
        
        assert "climate" in workflow.tags
        assert "preprocessing" in workflow.tags
        assert workflow.metadata["author"] == "test"
        assert workflow.metadata["version"] == "1.0"
        
        # Test adding tags
        workflow.add_tag("ocean")
        assert "ocean" in workflow.tags
        
        # Test removing tags  
        workflow.remove_tag("netcdf")
        assert "netcdf" not in workflow.tags
    
    def test_workflow_equality_and_hashing(self):
        """Test workflow equality and hashing."""
        workflow1 = WorkflowEntity(
            workflow_id="test-workflow",
            name="Test Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[]
        )
        
        workflow2 = WorkflowEntity(
            workflow_id="test-workflow",
            name="Different Name",  # Different name but same ID
            workflow_type=WorkflowType.MODEL_EXECUTION,  # Different type
            steps=[]
        )
        
        workflow3 = WorkflowEntity(
            workflow_id="different-workflow",
            name="Test Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[]
        )
        
        # Equal workflows (same ID)
        assert workflow1 == workflow2
        assert hash(workflow1) == hash(workflow2)
        
        # Different workflows (different ID)
        assert workflow1 != workflow3
        assert workflow1 != "not a workflow"
        
        # Can be used in sets
        workflow_set = {workflow1, workflow2, workflow3}
        assert len(workflow_set) == 2  # workflow1 and workflow2 are equal
    
    def test_workflow_string_representations(self):
        """Test string representations of workflow."""
        workflow = WorkflowEntity(
            workflow_id="test-workflow-001",
            name="Test Climate Workflow",
            workflow_type=WorkflowType.DATA_PREPROCESSING,
            steps=[],
            status=WorkflowStatus.READY
        )
        
        str_repr = str(workflow)
        assert "test-workflow-001" in str_repr
        assert "Test Climate Workflow" in str_repr
        assert "READY" in str_repr
        
        repr_str = repr(workflow)
        assert "WorkflowEntity" in repr_str
        assert "test-workflow-001" in repr_str