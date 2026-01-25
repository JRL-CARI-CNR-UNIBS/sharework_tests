# sharework_skills/launch/time_parametrization_wrapper.launch.py

import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration


def generate_launch_description():
    # These are the args expected by the included launch file :contentReference[oaicite:1]{index=1}
    config_pkg = LaunchConfiguration("config_pkg")
    config_file = LaunchConfiguration("config_file")

    tp_share = get_package_share_directory("time_parametrization")
    tp_launch = os.path.join(tp_share, "launch", "apply_time_parametrization_server.launch.py")

    return LaunchDescription(
        [
            DeclareLaunchArgument(
                "config_pkg",
                default_value="sharework_skills",
                description="Package whose share/ contains the YAML file",
            ),
            DeclareLaunchArgument(
                "config_file",
                default_value="config/time_parametrization_server.yaml",
                description="Path to YAML relative to config_pkg share/",
            ),
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(tp_launch),
                launch_arguments={
                    "config_pkg": config_pkg,
                    "config_file": config_file,
                }.items(),
            ),
        ]
    )

