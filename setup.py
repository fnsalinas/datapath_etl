from setuptools import setup, find_packages

setup(
    name='datapath01',
    version='1.0',
    description='Group No 6. Fisrst Python ETL project',
    # url='',
    author='Fabio Salinas',
    author_email='fabio.salinas1982@gmail.com',
    licence='MIT',
    packages=find_packages(
        where='src',
        include=['src', 'src.*']
    ),
    package_dir={
        '':'src'
    },
    zip_safe=False
)