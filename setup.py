from setuptools import find_packages, setup

import redshift_unloader


requirements = [name.strip() for name in open('requirements.txt').readlines()]
test_requirements = [name.strip() for name in open('requirements-test.txt').readlines()]
readme = open('README.md', 'r').read()


setup(
    name='redshift-unloader',
    version=redshift_unloader.__version__,
    description='Unload utility for Amazon Redshift',
    long_description=readme,
    author='Chaerim Yeo',
    author_email='yeochaerim@gmail.com',
    url='https://github.com/cryeo/redshift-unloader',
    license='MIT License',
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=requirements,
    tests_require=test_requirements,
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
