from tellus.testing.fixtures import sample_simulation_awi_locations_with_laptop

# Re-export the fixture to make it available to all tests
__all__ = ["sample_simulation_awi_locations_with_laptop"]

def trio_test(func):
    """Decorator to run async tests with Trio."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        return trio.run(func, *args, **kwargs)

    return wrapper
