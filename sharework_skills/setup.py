from setuptools import find_packages, setup
from glob import glob
import os

package_name = 'sharework_skills'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        (os.path.join('share', package_name, 'launch'), glob('launch/*.py')),
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
        'console_scripts': [
            'test_skills = sharework_skills.test_skills:main',
            'test_app = sharework_skills.test_app:main',
            'test_app_loop = sharework_skills.test_app_loop:main',
            'node = sharework_skills.main:main',
            'bt_node = sharework_skills.bt_pipeline:main',
        ],
    },
)
