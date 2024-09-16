from setuptools import find_packages, setup

setup(
    name="tellus",
    version="0.0.1",  # Initial placeholder version
    description="A framework for maintaining Earth System Model experiments",
    author="Paul Gierz",
    author_email="paul.gierz@awi.de",
    url="https://github.com/pgierz/tellus",  # Update this URL
    packages=find_packages(where="src"),  # Point to the src directory
    package_dir={"": "src"},  # Use the src layout
    python_requires=">=3.7",
)
