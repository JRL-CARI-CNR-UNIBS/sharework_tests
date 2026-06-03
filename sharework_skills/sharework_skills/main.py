#!/usr/bin/env python3
"""
main.py

Entry point for the pick & place pipeline node.
"""

import sys
import time

import rclpy

from .node import PoseConstraintsPipelineNode


def main(argv=None) -> None:
    rclpy.init(args=argv)
    node = None
    try:
        node = PoseConstraintsPipelineNode()
        node.run()

        # Keep the node alive for TF / RViz
        while rclpy.ok():
            rclpy.spin_once(node, timeout_sec=0.1)
            time.sleep(0.1)

    finally:
        if node is not None:
            node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main(sys.argv)
