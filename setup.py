from setuptools import setup, find_packages

setup(
    name="pyhmssql",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        # List your dependencies here
        "pytest",
        "pytest-cov",
    ],
)