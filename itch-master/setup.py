from setuptools import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
from subprocess import call
import os

# Call call to build the C file
os.chdir('ITCH/analysis/lag_gen')
call(['make'])
os.chdir('../../..')

cmdclass = {}
ext_modules = []

ext_modules += [Extension('ITCH.analysis.shrs_from_dict',
                          ['ITCH/analysis/cython/shrs_from_dict.pyx'])]
ext_modules += [Extension('ITCH.processing.decode',
                          ['ITCH/processing/cython/decode.pyx'])]

cmdclass.update({'build_ext': build_ext})

setup(
    name='ITCH',
    version='1.0',
    author='Scott Condie, Roy Roth, Robert Buss, Lehner White, Christopher Hair',
    author_email='sscondie@gmail.com',
    packages=['ITCH', 'ITCH.analysis', 'ITCH.processing'],
    url='https://bitbucket.org/byumcl/itch',
    description='Library for tools relating to NASDAQ ITCH data format analysis and processing',
    long_description=open('README.md').read(),
    cmdclass=cmdclass,
    ext_modules=ext_modules,
    install_requires=[
        "Cython >= 0.29",
        "NumPy >= 1.6.1",
        "pandas >= 0.13.1",
        "matplotlib",
        "scipy",
        "tqdm"
    ]
)
