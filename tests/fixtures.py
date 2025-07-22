from ..simulation import Simulation
import pytest


@pytest.fixture
def sample_simulation_awi_locations_with_laptop():
    # #########################################################################
    # [FIXME] Find out how to mock the part from here:
    # --------  8x  ---------
    # -------- SNIP  --------
    my_experiment = Simulation("my_experiment")
    # The experiment has a few linked locations:
    # * Tape storage (HSM)
    # * "disk" storage (direct on shared filesystems, e.g. /isibhv)
    # * "compute" storage (attached to compute infrastructure)
    my_experiment.add_location(
        {
            "name": "hsm",
            "hostname": "hsm.dmawi.de",
            "type": ["tape"],
        }
    )
    my_experiment.add_location(
        {
            "name": "albedo",
            "hostname": "albedo[0,1].dmawi.de",
            "type": ["compute", "disk"],
        }
    )
    my_experiment.add_location(
        {
            "name": "vm",
            "hostname": "hpcsrv[a,b,c].cloud.awi.de",
            "type": ["disk"],
        }
    )
    my_experiment.add_location(
        {
            "name": "laptop",
            "hostname": "binf02m082",
            "optional": True,
            "type": ["disk"],
        }
    )
    # -------- SNIP  --------
    # --------  x8  ---------
    # [FIXME] ...to here.
    return my_experiment
