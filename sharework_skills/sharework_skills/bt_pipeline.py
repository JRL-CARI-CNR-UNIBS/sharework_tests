"""
bt_pipeline.py  –  pick & place Behaviour Tree for pose_constraints_vla.yaml.

Tree topology is the ONLY thing defined here.
All ROS logic lives in robot_context.py and behaviours/.
"""

import yaml
from pathlib import Path

import py_trees
import py_trees_ros
import rclpy

from .robot_context import RobotContext
from .behaviours.motion import ExecuteMotionTask
from .behaviours.gripper import GripperCommand, FullCloseGuard
from .behaviours.perception import GetGrasps
from .behaviours.bag_recorder import StartBag, StopBag


def build_tree(ctx: RobotContext) -> py_trees_ros.trees.BehaviourTree:
    tasks = yaml.safe_load(Path(ctx.tasks_yaml_path).read_text())["tasks"]

    # ── split at grasp task ──────────────────────────────────────────────
    grasp_idx = next(
        i for i, t in enumerate(tasks)
        if t.get("type") == "grasp_detection/GetGrasps"
    )
    prefix_tasks = tasks[:grasp_idx]
    suffix_tasks = tasks[grasp_idx + 1:]

    # quick lookup by name
    t = {task["name"]: task for task in tasks}

    # ── PREFIX ──────────────────────────────────────────────────────────
    prefix = py_trees.composites.Sequence("Prefix", memory=True, children=[
        ExecuteMotionTask("goto_camera",  t["goto_camera"],  ctx),
        GripperCommand   ("open_gripper", t["open_gripper"], ctx),
    ])

    # ── PERCEPTION ──────────────────────────────────────────────────────
    perception = py_trees.decorators.Retry(
        name="PerceptionRetry",
        child=GetGrasps("get_grasp", ctx),
        num_failures=ctx.empty_grasp_retries,
    )

    # ── SUFFIX ──────────────────────────────────────────────────────────
    suffix = py_trees.composites.Sequence("Suffix", memory=True, children=[
        StartBag         ("start_bag_rec",   t["start_bag_rec"],    ctx),
        ExecuteMotionTask("move_to_approach", t["move_to_approach"], ctx),
        ExecuteMotionTask("move_to_object",   t["move_to_object"],   ctx),
        GripperCommand   ("close_gripper",    t["close_gripper"],    ctx),
        FullCloseGuard   ("full_close_check",                        ctx),
        ExecuteMotionTask("return",           t["return"],           ctx),
        StopBag          ("stop_bag_rec",     t["stop_bag_rec"],     ctx),
        ExecuteMotionTask("place",            t["place"],            ctx),
        ExecuteMotionTask("drop",             t["drop"],             ctx),
        GripperCommand   ("open_gripper2",    t["open_gripper2"],    ctx),
    ])

    # ── RECOVERY ────────────────────────────────────────────────────────
    # Always stops the bag (discard=True) before returning home.
    recovery = py_trees.composites.Sequence("Recovery", memory=False, children=[
        StopBag          ("recovery_stop_bag", {"discard": True},             ctx),
        GripperCommand   ("recovery_open",     {"position": 0.0,
                                                "max_effort": 50.0},          ctx),
        ExecuteMotionTask("recovery_return",   t["return"],                   ctx),
    ])

    # ── SUFFIX-OR-RECOVER ───────────────────────────────────────────────
    suffix_or_recover = py_trees.composites.Selector(
        "SuffixOrRecover", memory=False,
        children=[suffix, recovery],
    )

    # ── ONE CYCLE ───────────────────────────────────────────────────────
    cycle = py_trees.composites.Sequence("Cycle", memory=True, children=[
        prefix,
        perception,
        suffix_or_recover,
    ])

    # ── ROOT: repeat until GetGrasps keeps failing (scene empty) ────────
    root = py_trees.decorators.Repeat(
        "Root", child=cycle,
        num_success=ctx.max_recompute_attempts,
    )

    return py_trees_ros.trees.BehaviourTree(root, unicode_tree_debug=ctx.verbose)


def main() -> None:
    rclpy.init()
    ctx = RobotContext()
    ctx.wait_for_joint_state()
    ctx.wait_servers()

    tree = build_tree(ctx)
    tree.setup(timeout=ctx.wait_for_server_sec)
    tree.tick_tock(period_ms=200)

    try:
        rclpy.spin(tree.node)
    except KeyboardInterrupt:
        pass
    finally:
        tree.shutdown()
        rclpy.shutdown()
