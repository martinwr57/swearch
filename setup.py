#!/usr/bin/python
import setuptools

name = 'swearch'

setuptools.setup(
    name=name,
    version='2.3.6',
    description='',
    author='SoftLayer',
    packages=setuptools.find_packages(exclude=['ez_setup', 'examples',
                                               'tests']),
    install_requires=[],
    test_suite='tests',
    namespace_packages=[],
    entry_points={
        'paste.filter_factory': [
            'index=swearch.middleware.indexer:filter_factory',
            'search=swearch.middleware.searcher:filter_factory',
        ]
    },
    scripts=[
        'bin/swearch-prep',
        'bin/swearch-backfill',
        'bin/swearch-backfill-worker',
    ],
    zip_safe=False
)
