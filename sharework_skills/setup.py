from setuptools import find_packages, setup
from glob import glob
import os

package_name = 'sharework_skills'

setup(
    name=package_name,
    version='0.0.0',
    # Trova automaticamente la cartella sharework_skills e la installa come pacchetto Python
    packages=find_packages(exclude=['test']),
    data_files=[
        # Ament index + package.xml
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),

        # Installazione dei file di launch
        (os.path.join('share', package_name, 'launch'), glob('launch/*.py')),

        # Installazione dei file di configurazione YAML
        (os.path.join('share', package_name, 'config'), glob('config/*.yaml')),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='kalman',
    maintainer_email='manuel.beschi@unibs.it',
    description='Package per la gestione delle skill robotizzate e pose constraints pipeline',
    license='TODO: License declaration',
    extras_require={
        'test': [
            'pytest',
        ],
    },
    entry_points={
        # Unificato in un unico dizionario per non sovrascrivere le chiavi
        'console_scripts': [
            'test_skills = sharework_skills.test_skills:main',
            'test_app = sharework_skills.test_app:main',
            'test_app_loop = sharework_skills.test_app_loop:main',
            'node = sharework_skills.main:main'  # <-- Corretto: punta a main.py
        ],
    },
)