from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='almaqso',
    use_scm_version=True,
    description='ALMA QSO analysis',
    long_description=long_description,
    long_description_content_type='text/markdown',  # 使用するフォーマットを指定
    packages=find_packages(),
    install_requires=[
        'astropy',
        'numpy',
        'pandas',
        'pyvo',
        'scipy',
        'matplotlib',
        'astroquery'
    ],
)
