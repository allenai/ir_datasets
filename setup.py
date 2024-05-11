from glob import glob
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ir_datasets",
    version="0.5.7", # NOTE: keep this in sync with ir_datasets/__init__.py
    author="Sean MacAvaney",
    author_email="sean.macavaney@glasgow.ac.uk",
    description="provides a common interface to many IR ad-hoc ranking benchmarks, training datasets, etc.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/allenai/ir_datasets",
    include_package_data = True,
    packages=setuptools.find_packages(include=['ir_datasets', 'ir_datasets.**']),
    install_requires=list(open('requirements.txt')),
    classifiers=[],
    python_requires='>=3.8',
    entry_points={
        'console_scripts': ['ir_datasets=ir_datasets:main_cli'],
    },
    package_data={
        'ir_datasets': glob('docs/*.yaml') + glob('etc/*.json'),
        '': ['requirements.txt', 'LICENSE'],
    },
)
