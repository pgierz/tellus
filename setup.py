from setuptools import setup, find_packages

setup(
    name="tellus_chat",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "langchain>=0.1.0",
        "langchain-community>=0.0.10",
        "langchain-core>=0.1.0",
        "pydantic>=2.5.0",
        "python-dotenv>=1.0.0",
        "typer[all]>=0.9.0",
        "ollama>=0.1.5",
        "fastapi>=0.116.1,<0.117",
        "uvicorn>=0.35.0,<0.36",
    ],
    entry_points={
        "console_scripts": [
            "tellus-chat = tellus_chat.cli:main",
        ],
    },
)
