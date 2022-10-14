# Copyright (c) 2019, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import os

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

version = os.getenv('servicex_version')
if version is None:
    version = '0.1a1'
else:
    version = version.split('/')[-1]

setuptools.setup(
    name='servicex-transformer',
    packages=setuptools.find_packages(),
    version=version,
    license='bsd 3 clause',
    description='ServiceX Data Transformer for HEP Data',
    long_description=long_description,
    long_description_content_type="text/markdown",

    author='Ben Galewsky',
    author_email='bengal1@illinois.edu',
    url='https://github.com/ssl-hep/ServiceX_transformer',
    keywords=['HEP', 'Data Engineering', 'Data Lake'],
    install_requires=[
        'uproot==4.1.9',
        'awkward == 1.7.0',
        'requests==2.28.1',
        'pyarrow == 3.0.0',
        'numpy == 1.23.3',
        'pika==1.1.0',
        'minio==7.1.12',
        'retry == 0.9.2'
    ],

    extras_require={
        'test': [
            'pytest>=4.6.11',
            'coverage>=5.2',
            'codecov==2.1.8',
            'pytest-mock>=2.0.0',
            'flake8>=3.8'
        ]
    },

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
    python_requires='>=2.7',

)
