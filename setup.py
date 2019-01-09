from setuptools import setup, find_packages

with open('VERSION') as version_fd:
    version = version_fd.read().strip()

with open('README.md', 'r') as readme_fd:
    long_description = readme_fd.read()

setup(
    name='async-kinesis-client',
    version=version,
    description='Asynchronous Python client for AWS Kinesis',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/whale2/async-kinesis-client',

    install_requires=[
        'aioboto3==6.0.1',
        'multidict>=4.5.2'
    ],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    author='Nikita Makeev',
    author_email='whale2.box@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries'
    ]
)
