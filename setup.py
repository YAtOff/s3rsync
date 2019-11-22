from setuptools import setup, find_packages


setup(
    name="s3rsync",
    version="1.0.0",
    description="",
    packages=find_packages(exclude=("tests",)),
    zip_safe=False,
)
