# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import generate_dpe_annexes_scripts
setup(name=generate_dpe_annexes_scripts.__name__,
      version=generate_dpe_annexes_scripts.__version__,
      packages=find_packages(),
      url='https://sop-gitlab.cstb.local/DEE/tslab',
      install_requires=['pandas',
                        'numpy',
                        "rdflib",
                        "seaborn",
                        "h5py",
                        "paramiko",
                        "psutil",
                        ])

