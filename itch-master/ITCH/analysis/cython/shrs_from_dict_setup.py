from distutils.core import setup
from Cython.Build import cythonize

setup(name="shrs_from_dict", ext_modules=cythonize('shrs_from_dict.pyx'))
