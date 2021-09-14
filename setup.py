from setuptools import setup

setup(
    name="mp0",
    version="0.0.1",
    url="",
    author="",
    author_email="",
    maintainer="",
    long_description="",
    packages=["mp0"],
    include_package_data=True,
    python_requires=">=3.8,",
    install_requires=["fire==0.4.0"],
    extras_require={
        "dev": [
            "black",
        ]
    },
    entry_points={
        "console_scripts": ["mp0c = mp0.client:main", "mp0s = mp0.server:main"]
    },
)
