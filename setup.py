"""
setup
"""
import os
from setuptools import setup


def read(fname):
    """
    For read README file
    """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="sness",
    version="0.0.1",
    description="LuizaLabs' datalake lib ",
    keywords="spark gcp etl",
    install_requires=[
        "google-api-python-client==1.7.4", "google-api-core==1.3.0", "google-cloud==0.34.0",
        "google-cloud-storage==1.11.0", "google-cloud-bigquery==1.5.0", "pandas==0.23.4", "scikit-learn==0.19.2"
    ],
    packages=[
        'sness', 'sness.gcloud', 'sness.utils',
        'sness.config'
    ],
    long_description=read('readme.md'),
    classifiers=[
        "Topic :: Utilities",
        "License :: Other/Proprietary License",
    ],
    package_data={'': ['config/config.json']},
    include_package_data=True)