from setuptools import find_packages, setup
from glob import glob
import os

package_name = 'sharework_skills'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        # ament index + package.xml (keep as you have)
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),

        # install launch files
        (os.path.join('share', package_name, 'launch'), glob('launch/*.py')),

        # install config files (only needed if you store YAML in this package)
        (os.path.join('share', package_name, 'config'), glob('config/*.yaml')),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='kalman',
    maintainer_email='manuel.beschi@unibs.it',
    description='TODO: Package description',
    license='TODO: License declaration',
    extras_require={
        'test': [
            'pytest',
        ],
    },
    entry_points={
        'console_scripts': [
            'test_skills = sharework_skills.test_skills:main'
        ],
        'console_scripts': [
            'test_app = sharework_skills.test_app:main'
        ],
    },
)

