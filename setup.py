"""
Installs:
    - iiimets
"""

import codecs
from setuptools import setup
from setuptools import find_packages

with codecs.open('README.md', encoding='utf-8') as f:
    README = f.read()

setup(
    name='iiimets',
    version='0.2',
    description='IIIF to METS/MODS conversion script',
    long_description=README,
    long_description_content_type='text/markdown',
    author='Karl-Ulrich Krägelin',
    author_email='kraegelin@sub.uni-goettingen.de',
    url='https://github.com/karkraeg/iiimets',
    #license='MPL', MIT, BSD, GPL?
    packages=find_packages(),
    include_package_data=True,
    install_requires=open('requirements.txt').read().split('\n'),
    package_data={
        '': ['res/xslt/*.xsl', 'res/*.jar'],
    },
    entry_points={
        'console_scripts': [
            'iiimets=iiimets:main',
        ]
    },
)
