from pathlib import Path

from setuptools import find_packages, setup

with open('README.md') as readme_file:
    readme = readme_file.read()

with open(Path(__file__).parent / 'requirements.txt', 'r') as file:
    requirements = file.read().splitlines()

setup(
    author='Nutritional Lung Immunity',
    author_email='girder.developer@example.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description='A Girder plugin to run and manage NLI simulations',
    install_requires=requirements,
    license='Apache Software License 2.0',
    long_description=readme,
    long_description_content_type='text/x-rst',
    include_package_data=True,
    keywords='girder-plugin, nli',
    name='girder_nlisim',
    packages=find_packages(exclude=['test', 'test.*']),
    url='https://nutritionallungimmunity.org/',
    version='0.5.0',
    zip_safe=False,
    entry_points={'girder.plugin': ['nli = girder_nlisim.plugin:NLIGirderPlugin']},
)
