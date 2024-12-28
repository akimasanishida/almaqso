from setuptools import setup, find_packages

setup(
    name='almaqso',
    version='1.0',
    description='ALMA QSO analysis',
    packages=find_packages(),
    install_requires=[ 'astropy', 'numpy', 'pandas', 'pyvo', 'scipy', 'matplotlib' ],
)
