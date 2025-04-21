from setuptools import setup, find_packages

# We use setup.py for development compatibility
# Real metadata is in pyproject.toml
setup(
    name="bizzmq",
    packages=find_packages(),
)

