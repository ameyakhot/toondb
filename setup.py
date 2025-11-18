from setuptools import setup, find_packages

setup(
    name="toondb",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "python-toon>=0.1.3",
        "psycopg2-binary",  # For PostgreSQL
        "pymongo",          # For MongoDB
    ],
    python_requires=">=3.8",
)