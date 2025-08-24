from setuptools import find_packages, setup

setup(
    name="infra_platform",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "kafka-python",
        "requests",
        "pytz",
        "minio",
        "duckdb",
        "pandas",
    ],
)
