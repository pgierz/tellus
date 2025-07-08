class Simulation:
    """A Earth System Model Simulation"""

    def __init__(self, path):
        self.path = path
        self.attrs = {}
        self.data = None
        self.namelists = {}
        self.locations = {}
