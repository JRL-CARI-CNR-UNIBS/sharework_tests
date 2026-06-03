# Pick & Place Pipeline

A ROS 2 node that drives a robot arm through a fully configurable, YAML-driven
pick-and-place loop. It integrates MoveIt 2 motion planning, AnyGrasp-based grasp
detection, a Robotiq gripper, and custom geometric path constraints — all wired
together in a resilient cycle with automatic recovery.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [File Reference](#file-reference)
4. [Dependencies](#dependencies)
5. [Configuration — ROS Parameters](#configuration--ros-parameters)
6. [Task YAML Format](#task-yaml-format)
7. [Main Loop Explained](#main-loop-explained)
8. [Geometric Constraints](#geometric-constraints)
9. [Grasp Detection](#grasp-detection)
10. [Gripper Control](#gripper-control)
11. [Full-Close Detection](#full-close-detection)
12. [Error Handling & Recovery](#error-handling--recovery)
13. [TF Frames Published](#tf-frames-published)
14. [Topics & Actions](#topics--actions)
15. [Running the Node](#running-the-node)
16. [Dry-Run Mode](#dry-run-mode)
17. [Adding a New Task Type](#adding-a-new-task-type)

---

## Overview

The pipeline executes a repeating pick-and-place cycle:

```
[PREFIX tasks] → [Grasp detection] → [SUFFIX tasks]
     ↑                                      |
     └──────── recovery & retry ────────────┘
```

**PREFIX** moves the robot to the perception pose and opens the gripper.
**Grasp detection** calls AnyGrasp, scores candidates, and picks the best one.
**SUFFIX** moves to the approach pose, descends, grasps, lifts, places, and returns.

The cycle repeats until the scene is empty (no valid grasps found) or
`max_recompute_attempts` consecutive failures are reached.

---

## Architecture

```
main.py
  └── PoseConstraintsPipelineNode  (node.py)
        ├── gripper_service.py     — GripperCommand action wrapper
        ├── grasp_service.py       — /get_grasps service + TF transform
        ├── motion_service.py      — plan → time-parametrize → execute
        │     └── action_client.py — generic send_goal_and_wait helper
        ├── constraints.py         — pure MotionPlanRequest / GeometricConstraintArray builders
        ├── utils.py               — set_xyz, is_success
        └── exceptions.py          — FullCloseError
```

Each layer has a single responsibility and no circular imports.
`node.py` is the only file that holds mutable state (JointState cache, grasp pose,
action clients). All service modules are stateless functions.

---

## File Reference

### `main.py`
Entry point. Calls `rclpy.init`, instantiates `PoseConstraintsPipelineNode`,
calls `node.run()`, then keeps the node alive (spinning) so TF frames keep
broadcasting after the loop finishes. Calls `rclpy.shutdown` on exit.

### `node.py`
The ROS 2 node class `PoseConstraintsPipelineNode`. Responsibilities:

- **Parameter declaration and validation** — all ROS parameters are declared with
  defaults here. A `RuntimeError` is raised at startup for missing mandatory values.
- **JointState cache** — subscribes to the joint states topic with `BEST_EFFORT` QoS
  and keeps the latest message in `_latest_js`. The `_build_start_state_from_joint_states`
  method assembles a `RobotState` from it before every planning call.
- **TF broadcasting** — a 0.5 s timer broadcasts `grasp_frame`, `grasp_frame2`, and
  `approach_frame` whenever a grasp has been detected.
- **Task dispatch** — `_execute_task` reads the `type` field and routes to the
  appropriate service function.
- **Main loop** — `run()` orchestrates prefix / perception / suffix, retry logic, and
  recovery.

### `gripper_service.py`
`send_gripper_goal(node, client, position, max_effort) → (reached, stalled, position)`

Sends a `GripperCommand` goal and blocks until the result arrives. Returns a
3-tuple instead of raising on soft failures so the caller can inspect the result.
Returns `(False, False, nan)` when the goal is rejected or the result is unavailable.

### `grasp_service.py`
`call_get_grasps(node, client, tf_buffer, pose_publisher, *, top_k, wait_for_server_sec)`

Calls the `/get_grasps` service (AnyGrasp), transforms each returned pose from
the camera frame to `world` via TF2, and publishes all poses on `/grasp_poses`
for RViz debugging. Returns the top `top_k` candidates sorted by score descending
as a list of `(PoseStamped_cam, PoseStamped_world, score)` tuples.
Returns `[]` when no grasps are detected or TF is unavailable.

### `motion_service.py`
`plan_and_execute(node, plan_client, tp_client, exec_client, motion_plan_request, geometric_constraints, **kwargs)`

Three-step pipeline:

1. **Plan** — sends a `PlanWithConstraints` goal and checks `error_code`.
2. **Time parametrize** — sends an `ApplyTimeParametrization` goal; applies velocity
   and acceleration scaling and an optional Cartesian speed limit on a specific link.
3. **Execute** — sends an `ExecuteTrajectory` goal (skipped when `dry_run=True`).

Raises `RuntimeError` at the first failing step.

### `action_client.py`
`send_goal_and_wait(node, client, goal_msg, label) → result`

Single generic helper used by `motion_service.py` (and optionally elsewhere).
Sends a goal, spins until accepted and result is ready, raises `RuntimeError` if
the goal is rejected.

### `constraints.py`
Pure builder functions with no ROS side effects:

- `build_motion_plan_request(goal_configuration, start_state, ...)` — joint-space goal.
- `build_motion_plan_request_from_cartesian_goal(cartesian_goal, start_state, ...)` —
  Cartesian goal with position + orientation constraints.
- `build_geometric_constraints_array(geometric_constraints, stamp)` — builds a
  `GeometricConstraintArray` from a list of YAML-parsed dicts (see
  [Geometric Constraints](#geometric-constraints)).
- `yaml_type_to_uint8(t)` — internal helper that maps `"plane"` / `"line"` /
  `"angle"` strings to the corresponding `GeometricConstraint` integer constants.

### `utils.py`
- `set_xyz(obj, xyz, field_name)` — assigns a `[x, y, z]` list to an object with
  `.x/.y/.z` attributes. Raises with a descriptive message on bad input.
- `is_success(error_code_msg)` — returns `True` when a `MoveItErrorCodes` message
  equals `SUCCESS`.

### `exceptions.py`
- `FullCloseError(RuntimeError)` — raised when the gripper reaches full closure
  during or after a grasp attempt, indicating the object was not picked up.

---

## Dependencies

| Package | Purpose |
|---|---|
| `rclpy` | ROS 2 Python client library |
| `moveit_msgs` | `MotionPlanRequest`, `ExecuteTrajectory`, `MoveItErrorCodes` |
| `control_msgs` | `GripperCommand` action |
| `sensor_msgs` | `JointState` |
| `geometry_msgs` | `Pose`, `PoseStamped`, `TransformStamped` |
| `tf2_ros` | TF2 buffer, listener, broadcaster |
| `tf2_geometry_msgs` | `do_transform_pose` |
| `pose_constraints_msgs` | `PlanWithConstraints` action, `GeometricConstraint(Array)` |
| `time_parametrization_msgs` | `ApplyTimeParametrization` action |
| `grasp_detection_msgs` | `GetGrasps` service |
| `PyYAML` | Task file parsing |

---

## Configuration — ROS Parameters

All parameters are declared with defaults and can be overridden via a launch file
or on the command line with `--ros-args -p name:=value`.

### Required (no safe default)

| Parameter | Type | Description |
|---|---|---|
| `tasks_yaml_path` | `string` | Absolute path to the task definition YAML file. |
| `group_name` | `string` | MoveIt planning group name (e.g. `"manipulator"`). |
| `joint_names` | `string[]` | Ordered list of joint names for the planning group. |

### Joint State

| Parameter | Type | Default | Description |
|---|---|---|---|
| `joint_states_topic` | `string` | `/joint_states` | Topic on which `JointState` messages are published. |
| `wait_joint_states_sec` | `float` | `5.0` | How long to wait for the first `JointState` on startup. |
| `max_joint_state_age_sec` | `float` | `1.0` | Age threshold above which a warning is logged before planning. |

### Planning

| Parameter | Type | Default | Description |
|---|---|---|---|
| `joint_tolerance` | `float` | `0.001` | Position tolerance (rad) for joint goal constraints. |
| `num_planning_attempts` | `int` | `1` | Number of planning attempts per request. |
| `allowed_planning_time` | `float` | `15.0` | Maximum planning time in seconds. |
| `max_velocity_scaling_factor` | `float` | `0.1` | Velocity scaling (0–1). |
| `max_acceleration_scaling_factor` | `float` | `0.1` | Acceleration scaling (0–1). |
| `cartesian_speed_limited_link` | `string` | `""` | Link on which `max_cartesian_speed` is enforced. Empty = disabled. |
| `max_cartesian_speed` | `float` | `0.25` | Maximum Cartesian speed in m/s for the limited link. |

### Action Servers

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plan_action_name` | `string` | `/plan_with_constraints` | `PlanWithConstraints` action server. |
| `time_param_action_name` | `string` | `/apply_time_parametrization` | `ApplyTimeParametrization` action server. |
| `execute_action_name` | `string` | `/execute_trajectory` | `ExecuteTrajectory` action server. |
| `wait_for_server_sec` | `float` | `10.0` | Timeout when connecting to each action/service server. |

### Gripper

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gripper_joint_name` | `string` | `""` | Name of the gripper joint in `JointState`. Used for full-close detection. Empty = disabled. |
| `full_close_position` | `float` | `0.8` | Joint position (rad/m) that represents a fully closed gripper. |
| `full_close_tol` | `float` | `0.01` | Tolerance around `full_close_position` for the full-close check. |

### Grasp Detection

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min_grasp_score` | `float` | `0.0` | Minimum AnyGrasp confidence score to accept a candidate. |
| `empty_grasp_retries` | `int` | `15` | How many times to retry `/get_grasps` when no valid candidate is found. |
| `empty_grasp_retry_delay_sec` | `float` | `0.5` | Sleep between grasp detection retries. |
| `max_recompute_attempts` | `int` | `30` | Maximum consecutive suffix failures before the loop is aborted. |

### Behaviour

| Parameter | Type | Default | Description |
|---|---|---|---|
| `verbose` | `bool` | `False` | Pass `verbose=True` to the planner for extra debug output. |
| `stop_on_error` | `bool` | `True` | If `True`, re-raise any exception in the PREFIX phase and stop. If `False`, break the loop instead. |
| `dry_run` | `bool` | `False` | Plan and time-parametrize but skip execution. Useful for testing. |

---

## Task YAML Format

The YAML file must have a top-level `tasks` list. Each element is a task dict.
Tasks are split automatically around the single `grasp_detection/GetGrasps` entry:
everything before it is the **prefix**, everything after is the **suffix**.

```yaml
tasks:

  # ── PREFIX ──────────────────────────────────────────────────────────────────

  - name: open_gripper
    type: gripper_control/Gripper
    position: 0.0          # fully open
    max_effort: 50.0

  - name: home
    type: moveit/JointGoal
    goal_configuration: [0.0, -1.57, 1.57, -1.57, -1.57, 0.0]

  # ── GRASP DETECTION ─────────────────────────────────────────────────────────

  - name: detect_grasps
    type: grasp_detection/GetGrasps

  # ── SUFFIX ──────────────────────────────────────────────────────────────────

  - name: approach
    type: moveit/CartesianGoal
    cartesian_goal:
      frame_id: world
      link_name: tool0
      position: [0.45, 0.10, 0.50]
      orientation: [0.0, 0.707, 0.0, 0.707]

  - name: grasp
    type: gripper_control/Gripper
    position: 0.75
    max_effort: 100.0
    required_stall: true   # fail if the gripper does not stall against an object

  - name: lift
    type: moveit/JointGoal
    goal_configuration: [0.0, -1.2, 1.2, -1.57, -1.57, 0.0]
    geometric_constraints:
      - name: keep_upright
        type: plane
        frame: world
        origin: [0.0, 0.0, 0.0]
        normal: [0.0, 0.0, 1.0]
        plane_tolerance: 0.05

  # A task named exactly "return" is used by the recovery routine
  - name: return
    type: moveit/JointGoal
    goal_configuration: [0.0, -1.57, 1.57, -1.57, -1.57, 0.0]
```

### Task Types

| `type` value | Description |
|---|---|
| `gripper_control/Gripper` | Send a `GripperCommand`. Fields: `position` (float), `max_effort` (float), `required_stall` (bool, default `false`). |
| `moveit/JointGoal` | Plan to a joint configuration. Field: `goal_configuration` (list of floats, same length as `joint_names`). |
| `moveit/CartesianGoal` | Plan to a Cartesian pose. Field: `cartesian_goal` (dict — see below). |
| `grasp_detection/GetGrasps` | Trigger AnyGrasp. No extra fields. Must appear exactly once. |

#### `cartesian_goal` fields

| Field | Type | Description |
|---|---|---|
| `frame_id` | `string` | Reference frame for position and orientation. |
| `link_name` | `string` | End-effector link to constrain. |
| `position` | `[x, y, z]` | Target position in metres. |
| `orientation` | `[qx, qy, qz, qw]` | Target orientation as a quaternion. |

---

## Main Loop Explained

```
run()
│
├─ Wait for first JointState
├─ Wait for all action servers
├─ Load and split task YAML
│
└─ while rclpy.ok() and recompute_attempts < max_recompute_attempts:
      │
      ├─ [PREFIX]  execute prefix tasks in order
      │             → RuntimeError stops the loop (stop_on_error=True)
      │               or breaks it (stop_on_error=False)
      │
      ├─ [PERCEPTION]  call /get_grasps with retry
      │                 → no valid grasp after empty_grasp_retries → break (scene empty)
      │                 → select candidates[0] (highest score)
      │                 → broadcast TF frames
      │
      └─ [SUFFIX]  execute suffix tasks in order
                    → success: recompute_attempts=0, cycle_idx++
                    → FullCloseError or any exception:
                         recovery_open_and_return()
                         recompute_attempts++
                         → loop again from PREFIX
```

The key insight is that the **prefix** is re-executed on every recovery so the robot
always returns to a known perception pose before requesting a new grasp.

---

## Geometric Constraints

Geometric constraints are attached to any motion task via the optional
`geometric_constraints` list and passed to the `PlanWithConstraints` action.
They are defined by the `pose_constraints_msgs/GeometricConstraint` message.

Three types are supported:

### `plane`
Constrains a link to remain on one side of (or within a tolerance of) a plane.

```yaml
geometric_constraints:
  - name: keep_above_table
    type: plane
    frame: world
    origin: [0.0, 0.0, 0.72]   # a point on the plane
    normal: [0.0, 0.0, 1.0]    # plane normal (points up)
    plane_tolerance: 0.03       # allowed distance from the plane (metres)
```

### `line`
Constrains a link to stay within a maximum distance from a line.

```yaml
geometric_constraints:
  - name: stay_on_axis
    type: line
    frame: world
    origin: [0.0, 0.0, 0.0]
    direction: [0.0, 0.0, 1.0]
    max_distance: 0.1
```

### `angle` / `orientation`
Constrains the orientation of a link.

```yaml
geometric_constraints:
  - name: wrist_angle_limit
    type: angle
    frame: world
    max_angle: [0.1, 0.1, 3.14]   # max deviation per axis (rad)
```

---

## Grasp Detection

The node calls the `/get_grasps` service (expected type: `grasp_detection_msgs/srv/GetGrasps`)
and processes the response as follows:

1. All returned poses (in the camera frame) are transformed to `world` using TF2.
2. The full list is published on `/grasp_poses` (`geometry_msgs/PoseArray`) for RViz.
3. Candidates are sorted by score descending and truncated to `top_k=20`.
4. The **single best candidate** is selected (no fallback to lower-scored poses).

The node retries up to `empty_grasp_retries` times with a `empty_grasp_retry_delay_sec`
sleep between each attempt, both when the result is empty and when the best score is
below `min_grasp_score`.

---

## Gripper Control

The gripper is controlled via the `GripperCommand` action on
`/robotiq_action_controller/gripper_cmd`.

- **`position`** — target finger opening in the units expected by the controller
  (typically metres for a Robotiq 2F).
- **`max_effort`** — maximum force in Newtons.
- **`required_stall`** — when `true`, the task fails unless the gripper stalls
  (i.e. it contacts an object). Use this on the grasp close command.

The result tuple `(reached_goal, stalled, final_position)` is inspected to decide
whether to raise or continue.

---

## Full-Close Detection

Full-close detection catches the case where the gripper closes completely without
gripping an object (object dropped, missed, or not present).

Detection runs at two points:

1. **Immediately after a `gripper_control/Gripper` task** — the `final_position`
   returned by the action is compared to `full_close_position ± full_close_tol`.
2. **After every motion task in the suffix** — the live `JointState` value for
   `gripper_joint_name` is checked.

When full close is detected a `FullCloseError` is raised, which triggers the
[recovery routine](#error-handling--recovery).

Set `gripper_joint_name: ""` to disable this check entirely.

---

## Error Handling & Recovery

| Situation | Behaviour |
|---|---|
| PREFIX task fails | `RuntimeError` propagated (and re-raised if `stop_on_error=True`) or loop breaks. |
| No valid grasp after all retries | Loop exits cleanly ("scene empty"). |
| SUFFIX task fails (any exception incl. `FullCloseError`) | Recovery routine runs, `recompute_attempts` incremented. |
| `recompute_attempts >= max_recompute_attempts` | Loop aborts with an error log. |

### Recovery Routine (`_recovery_open_and_return`)

1. Send gripper open command (`position=0.0`, `max_effort=50.0`) — ignores errors
   so the robot is not stuck even if the gripper action is unresponsive.
2. Execute the task named exactly `"return"` from the suffix list (if present),
   temporarily disabling the full-close guard for that move.

After recovery the outer loop restarts from the PREFIX phase.

---

## TF Frames Published

A 0.5 s timer broadcasts three frames whenever a grasp has been detected:

| Frame ID | Parent | Description |
|---|---|---|
| `grasp_frame` | `world` | Grasp pose in world coordinates. Use this to command the approach. |
| `grasp_frame2` | camera frame | Same grasp pose in the camera frame (debug). |
| `approach_frame` | `world` | Grasp pose shifted +0.15 m along Z — safe pre-grasp position. |

These frames are available to any node or RViz plugin as long as the pipeline node
is running.

---

## Topics & Actions

### Subscribed Topics

| Topic | Type | Description |
|---|---|---|
| `/joint_states` (configurable) | `sensor_msgs/JointState` | Live joint positions. Used to build the start state for planning. |

### Published Topics

| Topic | Type | Description |
|---|---|---|
| `/grasp_poses` | `geometry_msgs/PoseArray` | All grasp candidates from the latest AnyGrasp call (camera frame). |

### Action Clients

| Action | Type | Description |
|---|---|---|
| `/plan_with_constraints` (configurable) | `pose_constraints_msgs/PlanWithConstraints` | Motion planning with geometric constraints. |
| `/apply_time_parametrization` (configurable) | `time_parametrization_msgs/ApplyTimeParametrization` | Trajectory time scaling. |
| `/execute_trajectory` (configurable) | `moveit_msgs/ExecuteTrajectory` | Trajectory execution on the hardware/sim. |
| `/robotiq_action_controller/gripper_cmd` | `control_msgs/GripperCommand` | Gripper open/close commands. |

### Service Clients

| Service | Type | Description |
|---|---|---|
| `/get_grasps` | `grasp_detection_msgs/GetGrasps` | AnyGrasp detection request. |

---

## Running the Node

```bash
# Minimal launch (all other params use defaults)
ros2 run <your_package> main.py \
  --ros-args \
  -p tasks_yaml_path:=/path/to/tasks.yaml \
  -p group_name:=manipulator \
  -p joint_names:="['joint_1','joint_2','joint_3','joint_4','joint_5','joint_6']"
```

Or with a launch file:

```python
from launch import LaunchDescription
from launch_ros.actions import Node

def generate_launch_description():
    return LaunchDescription([
        Node(
            package="your_package",
            executable="main.py",
            name="pose_constraints_pipeline",
            parameters=[{
                "tasks_yaml_path": "/path/to/tasks.yaml",
                "group_name": "manipulator",
                "joint_names": ["joint_1", "joint_2", "joint_3",
                                "joint_4", "joint_5", "joint_6"],
                "max_velocity_scaling_factor": 0.2,
                "max_acceleration_scaling_factor": 0.2,
                "min_grasp_score": 0.5,
                "dry_run": False,
            }],
        )
    ])
```

---

## Dry-Run Mode

Set `dry_run:=true` to run the full planning and time-parametrization pipeline
without sending any trajectory to the hardware. The gripper action **is** still sent.
This is useful for:

- Verifying that all tasks plan successfully from a given configuration.
- Checking that geometric constraints do not prevent planning.
- Debugging the grasp detection pipeline without moving the robot.

---

## Adding a New Task Type

1. Add a new `elif ttype == "your_package/YourType":` block in
   `node.py::_execute_task`.
2. Extract any required fields from the task dict `t`.
3. Call the appropriate service function (add a new file under `*_service.py` if
   the logic is non-trivial).
4. Document the new task type in the [Task YAML Format](#task-yaml-format) section
   and in the YAML file itself.

No changes to `main.py`, `constraints.py`, `utils.py`, or `exceptions.py` are needed
for a pure new task type.
