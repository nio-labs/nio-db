from setuptools import setup, find_packages

setup(
    name="nio-db-py",
    version="1.0.0",
    description="Ultra-lightweight Python client for NioDB (The Agentic Database)",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Nio Labs",
    license="MIT",
    packages=find_packages(exclude=["tests*"]),
    python_requires=">=3.8",
    keywords=["niodb", "database", "ai-agents", "vector-search", "sqlite", "rag"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
