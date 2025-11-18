from setuptools import setup, find_packages

# Read README for long description
try:
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()
except FileNotFoundError:
    long_description = ""

setup(
    name="toondb",
    version="0.1.6",
    author="Ameya Khot",
    author_email="ameyakhot18@gmail.com",
    description="Multi-database adapter library that converts query results to TOON format for efficient LLM usage",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ameyakhot/toondb",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    install_requires=[
        "python-toon>=0.1.3",
        "tiktoken>=0.5.0",
    ],
    extras_require={
        "postgres": ["psycopg2-binary>=2.9.0"],
        "mysql": ["pymysql>=1.0.0"],
        "mongodb": ["pymongo>=4.0.0"],
        "all": ["psycopg2-binary>=2.9.0", "pymysql>=1.0.0", "pymongo>=4.0.0"],
    },
    python_requires=">=3.8",
    keywords="toon, database, adapter, llm, token-efficient, postgresql, mysql, mongodb, token-oriented-object-notation",
    project_urls={
        "Bug Reports": "https://github.com/ameyakhot/toondb/issues",
        "Source": "https://github.com/ameyakhot/toondb",
        "Documentation": "https://github.com/ameyakhot/toondb#readme",
    },
)
