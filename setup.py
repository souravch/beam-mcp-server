from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

def parse_requirements(filename):
    """Parse requirements from requirements.txt file."""
    with open(filename, "r", encoding="utf-8") as f:
        requirements = []
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                # Remove any comments
                line = line.split('#')[0].strip()
                # Skip editable installs
                if not line.startswith('-e'):
                    requirements.append(line)
        return requirements

# Get requirements from requirements.txt
requirements = parse_requirements("requirements.txt")

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
    extras_require={
        "dev": [
            # Testing and development
            "pytest>=7.0.0,<8.0.0",
            "pytest-asyncio>=0.16.0,<1.0.0",
            "flake8>=6.1.0",
            "marshmallow>=3.20.1",
            "kubernetes>=28.1.0",
            "gitpython>=3.1.40",
            
            # Code formatting and type checking
            "black>=22.0.0,<23.0.0",
            "isort>=5.10.0,<6.0.0",
            "mypy>=1.0.0,<2.0.0",
            "types-PyYAML>=6.0.0,<7.0.0",
            "types-requests>=2.26.0,<3.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "beam-mcp-server=src.main:main",
        ],
    },
) 