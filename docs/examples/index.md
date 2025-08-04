# Examples

This section contains practical examples and tutorials showing how to use Tellus in real-world scenarios. All examples are provided as interactive Jupyter notebooks that you can run and modify.

## Available Examples

::::{grid} 1 2 2 2
:::{grid-item-card} {octicon}`play` Basic Usage
:link: basic-usage
:link-type: doc

Learn the fundamentals with a step-by-step tutorial covering simulations, locations, and file operations.
:::

:::{grid-item-card} {octicon}`cloud` Remote Data Access
:link: remote-data
:link-type: doc

Access data from SSH servers, cloud storage, and other remote locations with authentication and progress tracking.
:::

:::{grid-item-card} {octicon}`workflow` Workflow Integration
:link: workflow-integration
:link-type: doc

Integrate Tellus with Snakemake workflows for automated data management in computational pipelines.
:::

:::{grid-item-card} {octicon}`database` Archive System - Getting Started
:link: archive-system-getting-started
:link-type: doc

Learn the basics of Tellus's intelligent archive system with automatic caching, tagging, and file extraction.
:::

:::{grid-item-card} {octicon}`gear` Archive System - Advanced Features
:link: archive-system-advanced-features
:link-type: doc

Explore advanced features including location integration, custom tagging, path mapping, and production deployment.
:::

:::{grid-item-card} {octicon}`terminal` Archive System - CLI Examples
:link: archive-system-cli-examples
:link-type: doc

Master the command-line interface for archive operations, batch processing, and workflow automation.
:::
::::

## Running the Examples

All examples are provided as Jupyter notebooks (`.ipynb` files) that you can run interactively. Here's how to get started:

### Option 1: Run Locally

```bash
# Install Jupyter in your Tellus environment
pixi add jupyter

# Start Jupyter
jupyter notebook docs/examples/
```

### Option 2: View Online

You can view the rendered notebooks directly in this documentation. The notebooks are executed automatically when the documentation is built, so you can see the outputs without running them yourself.

### Option 3: Download and Modify

Each example page has a download button that lets you save the notebook file to your local machine for modification and experimentation.

## Example Data

Some examples use sample datasets. These are either:

- Generated programmatically within the notebook
- Downloaded from public repositories
- Created as mock data for demonstration purposes

No real scientific data is included in the repository to keep it lightweight.

## Contributing Examples

We welcome contributions of new examples! If you have a use case that would be helpful to others:

1. Create a Jupyter notebook following our example format
2. Include clear explanations and comments
3. Test that it runs completely from start to finish
4. Submit a pull request

See our {doc}`../development/contributing` guide for more details.

```{toctree}
:maxdepth: 2

basic-usage
remote-data  
workflow-integration
archive-system-getting-started
archive-system-advanced-features
archive-system-cli-examples
```