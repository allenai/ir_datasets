from glob import glob
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ir_datasets",
    version="0.4.2", # NOTE: keep this in sync with ir_datasets/__init__.py
    author="Sean MacAvaney",
    author_email="sean.macavaney@glasgow.ac.uk",
    description="provides a common interface to many IR ad-hoc ranking benchmarks, training datasets, etc.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/allenai/ir_datasets",
    include_package_data = True,
    packages=setuptools.find_packages(include=['ir_datasets', 'ir_datasets.*']),
    install_requires=list(open('requirements.txt')),
    classifiers=[],
    python_requires='>=3.6',
    extras_require={"datamaestro": ["datamaestro_text>=2021.7.19", "datamaestro>=0.8.0"]},
    entry_points={
        'console_scripts': ['ir_datasets=ir_datasets:main_cli'],
        'datamaestro.repositories': ['irds=ir_datasets.datamaestro:Repository']
    },
    package_data={
        'ir_datasets': glob('docs/*.yaml') + glob('etc/*.json'),
        '': ['requirements.txt', 'LICENSE'],
    },
)
