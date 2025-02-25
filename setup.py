from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = [line.strip() for line in f.readlines()]

setup(
    name="beam-mcp-server",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A Model Context Protocol (MCP) server for Apache Beam pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/beam-mcp-server",
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "beam-mcp-server=src.main:main",
        ],
    },
) 