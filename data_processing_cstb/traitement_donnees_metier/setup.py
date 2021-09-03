# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import generate_dpe_annexes_scripts
setup(name=generate_dpe_annexes_scripts.__name__,
      version=generate_dpe_annexes_scripts.__version__,
      packages=find_packages(),
      url='https://github.com/etalab/de-dpe-processing/tree/cstb_traitements_metier_v3/data_processing_cstb',
      install_requires=['pandas',
                        'numpy',
                        "seaborn",
                        ])

