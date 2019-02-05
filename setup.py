# Always prefer setuptools over distutils
from setuptools import setup, find_packages

# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='oracle2postgres',

    # Versions should comply with PEP440. For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version='0.1.6',

    description='Migrate data from an Oracle to Postgres',
    long_description=long_description,
    long_description_content_type='text/markdown',

    # The project's main homepage.
    url='https://github.com/MIT-LCP/oracle-to-postgres',

    # Author details
    author='Tom Pollard',
    author_email='tpollard@mit.edu',

    # Choose your license
    # license='MIT',

    # What does your project relate to?
    keywords='Migrate oracle postgres database migration',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    # packages=find_packages(exclude=['contrib', 'docs', 'tests','data','env',
    #     'output','trained_models']),
    # packages=['migrate'],

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    py_modules=['oracle2postgres'],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=[
        'cx-Oracle>=7.0.0',
        'DateTime>=4.3',
        'numpy>=1.16.1',
        'pandas>=0.24.0',
        'psycopg2>=2.7.7',
        'python-dateutil>=2.7.5',
        'pytz>=2018.9',
        'six>=1.12.0',
        'SQLAlchemy>=1.2.17',
        'zope.interface>=4.6.0'
        ],

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    # extras_require={
    #     'dev': ['check-manifest'],
    #     'test': ['coverage'],
    # },

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    # package_data={'wfdb': ['wfdb.config'],
    # },

    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages. See:
    # http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files # noqa
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
    # data_files=[('my_data', ['data/data_file'])],
    # data_files=[('config', ['wfdb.config'])],

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    # entry_points={
    #     'console_scripts': [
    #         'sample=sample:main',
    #     ],
    # },

)