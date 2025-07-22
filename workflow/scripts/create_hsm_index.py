import sys
import json
from pathlib import Path


def create_hsm_index(output_path, config, wildcards):
    """
    Create an HSM index file based on the experiment configuration.

    Args:
        output_path: Path to the output file
        config: Snakemake config dictionary
        wildcards: Snakemake wildcards
    """
    try:
        # Get experiment config
        exp_config = config["experiments"][wildcards.expid]
        location_config = exp_config["locations"][wildcards.hsm_host]

        # Create index data
        index_data = {
            "experiment_id": wildcards.expid,
            "host": wildcards.hsm_host,
            "path": location_config["path"],
            "created_at": "TODO: Add timestamp",
            "files": [],  # TODO: Add logic to discover files
        }

        # Write to output file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(index_data, f, indent=2)

        print(f"Created HSM index file at {output_path}")

    except Exception as e:
        print(f"Error creating HSM index: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    # Parse command line arguments
    output_path = Path(sys.argv[1])
    config = json.loads(sys.argv[2])
    wildcards = json.loads(sys.argv[3])

    create_hsm_index(output_path, config, wildcards)
