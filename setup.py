from setuptools import setup, find_packages


# Helper function to read dependencies from requirements.txt
def parse_requirements(filename):
    with open(filename, "r") as f:
        return [line.strip() for line in f if line and not line.startswith("#")]


setup(
    name="etl_ml_pipeline",
    version="0.1.0",
    packages=find_packages(include=["etl", "etl.*"]),
    install_requires=parse_requirements("requirements.txt"),
    python_requires=">=3.10",
)
