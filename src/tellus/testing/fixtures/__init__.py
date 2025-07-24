import pytest

from ...simulation import Simulation


@pytest.fixture
def sample_simulation_awi_locations_with_laptop():
    # #########################################################################
    my_experiment = Simulation("my_experiment")
    # The experiment has a few linked locations:
    # * Tape storage (HSM)
    # * "disk" storage (direct on shared filesystems, e.g. /isibhv)
    # * "compute" storage (attached to compute infrastructure)
    my_experiment.add_location(
        {
            "name": "hsm",
            "kinds": ["tape"],
            "config": {
                "protocol": "scoutfs",
                "storage_options": {
                    "host": "hsm.dmawi.de",
                },
            },
        }
    )
    my_experiment.add_location(
        {
            "name": "albedo",
            "kinds": ["compute", "disk"],
            "config": {
                "protocol": "file",
                "storage_options": {
                    "host": "albedo[0,1].dmawi.de",
                },
            },
        }
    )
    my_experiment.add_location(
        {
            "name": "vm",
            "kinds": ["disk"],
            "config": {
                "protocol": "file",
                "storage_options": {
                    "host": "hpcsrv[a,b,c].dmawi.de",
                },
            },
        }
    )
    my_experiment.add_location(
        {
            "name": "laptop",
            "optional": True,
            "kinds": ["disk"],
            "config": {
                "protocol": "file",
                "storage_options": {
                    "host": "binf02m082.dmawi.de",
                },
            },
        }
    )
    return my_experiment
