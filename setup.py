from setuptools import find_packages, setup

setup(
    name="berkeley_crime",
    packages=find_packages(exclude=["berkeley_crime_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
