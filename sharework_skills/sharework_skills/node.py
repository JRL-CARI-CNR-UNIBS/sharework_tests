"""
node.py

PoseConstraintsPipelineNode — the ROS 2 node for the pick & place pipeline.

Responsibilities:
  - Declare and read all ROS parameters
  - Maintain the JointState cache and expose a RobotState builder
  - Publish grasp TF frames
  - Orchestrate the main pick & place loop (prefix → perception → suffix)
  - Delegate action / service calls to services.py
  - Delegate constraint building to constraints.py
"""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import rclpy
import rclpy.duration
import yaml
from control_msgs.action import GripperCommand
from geometry_msgs.msg import PoseArray, PoseStamped, TransformStamped
from moveit_msgs.msg import RobotState
from pose_constraints_msgs.action import PlanWithConstraints
from rclpy.action import ActionClient
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy
from sensor_msgs.msg import JointState
from tf2_ros import TransformBroadcaster, TransformListener
from tf2_ros.buffer import Buffer
from time_parametrization_msgs.action import ApplyTimeParametrization
from moveit_msgs.action import ExecuteTrajectory
from bag_recorder_msgs.srv import StartRecording, StopRecording
from grasp_detection_msgs.srv import GetGrasps

from .bag_recorder_service import start_bag_recording, stop_bag_recording
from .constraints import (
    build_geometric_constraints_array,
    build_motion_plan_request,
    build_motion_plan_request_from_cartesian_goal,
)
from .exceptions import FullCloseError
from .grasp_service import call_get_grasps
from .gripper_service import send_gripper_goal
from .motion_service import plan_and_execute


class PoseConstraintsPipelineNode(Node):
    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self) -> None:
        super().__init__("pose_constraints_pipeline")
        self._declare_parameters()
        self._read_parameters()
        self._validate_parameters()
        self._init_ros_interfaces()

    def _declare_parameters(self) -> None:
        self.declare_parameter("tasks_yaml_path", "")
        self.declare_parameter("group_name", "")
        self.declare_parameter("joint_names", [""])

        self.declare_parameter("joint_states_topic", "/joint_states")
        self.declare_parameter("wait_joint_states_sec", 5.0)
        self.declare_parameter("max_joint_state_age_sec", 1.0)

        self.declare_parameter("joint_tolerance", 0.001)
        self.declare_parameter("num_planning_attempts", 1)
        self.declare_parameter("allowed_planning_time", 15.0)

        self.declare_parameter("max_velocity_scaling_factor", 0.1)
        self.declare_parameter("max_acceleration_scaling_factor", 0.1)

        self.declare_parameter("cartesian_speed_limited_link", "")
        self.declare_parameter("max_cartesian_speed", 0.25)

        self.declare_parameter("verbose", False)
        self.declare_parameter("stop_on_error", True)
        self.declare_parameter("dry_run", False)

        self.declare_parameter("plan_action_name", "/plan_with_constraints")
        self.declare_parameter("time_param_action_name", "/apply_time_parametrization")
        self.declare_parameter("execute_action_name", "/execute_trajectory")
        self.declare_parameter("wait_for_server_sec", 10.0)

        self.declare_parameter("gripper_joint_name", "")
        self.declare_parameter("full_close_position", 0.8)
        self.declare_parameter("full_close_tol", 0.01)

        self.declare_parameter("min_grasp_score", 0.0)
        self.declare_parameter("empty_grasp_retries", 15)
        self.declare_parameter("empty_grasp_retry_delay_sec", 0.5)
        self.declare_parameter("max_recompute_attempts", 30)

    def _read_parameters(self) -> None:
        self.tasks_yaml_path = str(self.get_parameter("tasks_yaml_path").value)
        self.group_name = str(self.get_parameter("group_name").value)
        self.get_logger().info(f"group_name = {self.group_name}")

        self.joint_names = [s for s in list(self.get_parameter("joint_names").value) if str(s).strip()]

        self.joint_states_topic = str(self.get_parameter("joint_states_topic").value)
        self.wait_joint_states_sec = float(self.get_parameter("wait_joint_states_sec").value)
        self.max_joint_state_age_sec = float(self.get_parameter("max_joint_state_age_sec").value)

        self.joint_tolerance = float(self.get_parameter("joint_tolerance").value)
        self.num_planning_attempts = int(self.get_parameter("num_planning_attempts").value)
        self.allowed_planning_time = float(self.get_parameter("allowed_planning_time").value)

        self.max_vel = float(self.get_parameter("max_velocity_scaling_factor").value)
        self.max_acc = float(self.get_parameter("max_acceleration_scaling_factor").value)

        self.cartesian_speed_limited_link = str(self.get_parameter("cartesian_speed_limited_link").value)
        self.max_cartesian_speed = float(self.get_parameter("max_cartesian_speed").value)

        self.verbose = bool(self.get_parameter("verbose").value)
        self.stop_on_error = bool(self.get_parameter("stop_on_error").value)
        self.dry_run = bool(self.get_parameter("dry_run").value)

        self.plan_action_name = str(self.get_parameter("plan_action_name").value)
        self.time_param_action_name = str(self.get_parameter("time_param_action_name").value)
        self.execute_action_name = str(self.get_parameter("execute_action_name").value)
        self.wait_for_server_sec = float(self.get_parameter("wait_for_server_sec").value)

        self.gripper_joint_name = str(self.get_parameter("gripper_joint_name").value)
        self.full_close_position = float(self.get_parameter("full_close_position").value)
        self.full_close_tol = float(self.get_parameter("full_close_tol").value)

        self.min_grasp_score = float(self.get_parameter("min_grasp_score").value)
        self.empty_grasp_retries = int(self.get_parameter("empty_grasp_retries").value)
        self.empty_grasp_retry_delay_sec = float(self.get_parameter("empty_grasp_retry_delay_sec").value)
        self.max_recompute_attempts = int(self.get_parameter("max_recompute_attempts").value)

    def _validate_parameters(self) -> None:
        if not self.tasks_yaml_path:
            raise RuntimeError("Parametro 'tasks_yaml_path' vuoto.")
        if not self.group_name:
            raise RuntimeError("Parametro 'group_name' vuoto.")
        if not self.joint_names:
            raise RuntimeError("Parametro 'joint_names' vuoto.")

    def _init_ros_interfaces(self) -> None:
        # JointState cache
        self._latest_js: Optional[JointState] = None
        self._latest_js_rx_time = None

        qos = QoSProfile(depth=10)
        qos.reliability = ReliabilityPolicy.BEST_EFFORT
        qos.durability = DurabilityPolicy.VOLATILE
        self._js_sub = self.create_subscription(
            JointState, self.joint_states_topic, self._on_joint_state, qos
        )

        # Action clients
        self.plan_client = ActionClient(self, PlanWithConstraints, self.plan_action_name)
        self.tp_client = ActionClient(self, ApplyTimeParametrization, self.time_param_action_name)
        self.exec_client = ActionClient(self, ExecuteTrajectory, self.execute_action_name)
        self.gripper_client = ActionClient(self, GripperCommand, "/robotiq_action_controller/gripper_cmd")

        # Bag recorder service clients
        self.bag_start_client = self.create_client(StartRecording, "/bag_recorder/start")
        self.bag_stop_client = self.create_client(StopRecording, "/bag_recorder/stop")
        self._active_bag_name: Optional[str] = None   # set by start_bag, consumed by stop_bag

        # Grasp service client + debug publisher + TF
        self.grasp_client = self.create_client(GetGrasps, "/get_grasps")
        self.grasp_pose_publisher = self.create_publisher(PoseArray, "/grasp_poses", 10)

        self.tf_broadcaster = TransformBroadcaster(self)
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)

        self.grasp_pose: Optional[PoseStamped] = None
        self.grasp_pose_in_world: Optional[PoseStamped] = None
        self.tf_timer = self.create_timer(0.5, self._publish_grasp_transforms)

        # Suffix guard flag
        self._in_candidate_suffix = False

    # ------------------------------------------------------------------
    # JointState helpers
    # ------------------------------------------------------------------

    def _on_joint_state(self, msg: JointState) -> None:
        self._latest_js = msg
        self._latest_js_rx_time = self.get_clock().now()

    def _wait_for_joint_state(self) -> None:
        deadline = self.get_clock().now() + rclpy.duration.Duration(seconds=self.wait_joint_states_sec)
        while rclpy.ok() and self._latest_js is None and self.get_clock().now() < deadline:
            rclpy.spin_once(self, timeout_sec=0.1)
        if self._latest_js is None:
            raise RuntimeError(
                f"Non ho ricevuto alcun JointState su '{self.joint_states_topic}' "
                f"entro {self.wait_joint_states_sec}s."
            )

    def _build_start_state_from_joint_states(self) -> RobotState:
        if self._latest_js is None:
            raise RuntimeError("JointState non disponibile (cache vuota).")

        js = self._latest_js

        try:
            age = (self.get_clock().now() - self._latest_js_rx_time).nanoseconds / 1e9
            if age > self.max_joint_state_age_sec:
                self.get_logger().warn(
                    f"JointState ricevuto {age:.3f}s fa (> {self.max_joint_state_age_sec:.3f}s). "
                    "Potrebbe non rappresentare lo stato corrente."
                )
        except Exception:
            pass

        if not js.name or not js.position:
            raise RuntimeError("JointState name/position vuoti sul topic joint_states.")

        name_to_index = {n: i for i, n in enumerate(js.name)}
        ordered_pos: List[float] = []
        missing: List[str] = []
        for jn in self.joint_names:
            if jn not in name_to_index:
                missing.append(jn)
                continue
            idx = name_to_index[jn]
            if idx >= len(js.position):
                missing.append(jn)
                continue
            ordered_pos.append(float(js.position[idx]))

        if missing:
            raise RuntimeError(f"JointState non contiene tutti i giunti richiesti. Mancanti: {missing}")

        start = RobotState()
        start.joint_state.name = list(self.joint_names)
        start.joint_state.position = ordered_pos
        try:
            start.joint_state.header.stamp = js.header.stamp
        except Exception:
            pass
        try:
            start.is_diff = False
        except Exception:
            pass
        return start

    # ------------------------------------------------------------------
    # Full-close helpers
    # ------------------------------------------------------------------

    def _is_full_close_value(self, value: float) -> bool:
        if value != value:  # NaN guard
            return False
        return abs(float(value) - float(self.full_close_position)) <= float(self.full_close_tol)

    def _get_joint_position(self, joint_name: str) -> Optional[float]:
        if not joint_name or self._latest_js is None:
            return None
        try:
            idx = self._latest_js.name.index(joint_name)
            return float(self._latest_js.position[idx])
        except Exception:
            return None

    def _raise_if_gripper_full_close_from_joint_states(self, where: str) -> None:
        if not self.gripper_joint_name:
            return
        pos = self._get_joint_position(self.gripper_joint_name)
        if pos is None:
            return
        if self._is_full_close_value(pos):
            raise FullCloseError(
                f"[FULL CLOSE] (joint_states) {where}: "
                f"{self.gripper_joint_name}={pos:.4f} (~={self.full_close_position:.4f})"
            )

    # ------------------------------------------------------------------
    # Server readiness
    # ------------------------------------------------------------------

    def _wait_servers(self) -> None:
        for client, name in [
            (self.plan_client, self.plan_action_name),
            (self.tp_client, self.time_param_action_name),
            (self.exec_client, self.execute_action_name),
            (self.gripper_client, "/robotiq_action_controller/gripper_cmd"),
        ]:
            self.get_logger().info(f"Attendo action server: {name}")
            if not client.wait_for_server(timeout_sec=self.wait_for_server_sec):
                raise RuntimeError(
                    f"Action server non disponibile entro {self.wait_for_server_sec}s: {name}"
                )

    # ------------------------------------------------------------------
    # YAML task loading
    # ------------------------------------------------------------------

    def _load_tasks(self) -> List[Dict[str, Any]]:
        p = Path(self.tasks_yaml_path)
        if not p.exists():
            raise RuntimeError(f"File YAML non trovato: {p}")
        data = yaml.safe_load(p.read_text())
        if not isinstance(data, dict) or "tasks" not in data:
            raise RuntimeError("YAML non valido: atteso mapping con chiave 'tasks'.")
        tasks = data["tasks"]
        if not isinstance(tasks, list):
            raise RuntimeError("YAML non valido: 'tasks' deve essere una lista.")
        return tasks

    # ------------------------------------------------------------------
    # TF – grasp frame publishing
    # ------------------------------------------------------------------

    def _set_current_grasp(self, pose_cam: PoseStamped, pose_world: PoseStamped) -> None:
        self.grasp_pose = pose_cam
        self.grasp_pose_in_world = pose_world
        self._publish_grasp_transforms()

    def _publish_grasp_transforms(self) -> None:
        """Timer callback: broadcast grasp_frame, grasp_frame2, approach_frame."""
        if not self.grasp_pose or not self.grasp_pose_in_world:
            return

        now = self.get_clock().now().to_msg()

        # grasp_frame2 in camera frame (debug)
        grasp_tf_cam = TransformStamped()
        grasp_tf_cam.header = self.grasp_pose.header
        grasp_tf_cam.header.stamp = now
        grasp_tf_cam.child_frame_id = "grasp_frame2"
        grasp_tf_cam.transform.translation.x = self.grasp_pose.pose.position.x
        grasp_tf_cam.transform.translation.y = self.grasp_pose.pose.position.y
        grasp_tf_cam.transform.translation.z = self.grasp_pose.pose.position.z
        grasp_tf_cam.transform.rotation = self.grasp_pose.pose.orientation
        self.tf_broadcaster.sendTransform(grasp_tf_cam)

        # grasp_frame in world frame
        grasp_tf_world = TransformStamped()
        grasp_tf_world.header = self.grasp_pose_in_world.header
        grasp_tf_world.header.stamp = now
        grasp_tf_world.child_frame_id = "grasp_frame"
        grasp_tf_world.transform.translation.x = self.grasp_pose_in_world.pose.position.x
        grasp_tf_world.transform.translation.y = self.grasp_pose_in_world.pose.position.y
        grasp_tf_world.transform.translation.z = self.grasp_pose_in_world.pose.position.z
        grasp_tf_world.transform.rotation = self.grasp_pose_in_world.pose.orientation
        self.tf_broadcaster.sendTransform(grasp_tf_world)

        # approach_frame: 0.15 m above grasp in world frame
        approach_tf = TransformStamped()
        approach_tf.header = self.grasp_pose_in_world.header
        approach_tf.header.stamp = now
        approach_tf.child_frame_id = "approach_frame"
        approach_tf.transform.translation.x = self.grasp_pose_in_world.pose.position.x
        approach_tf.transform.translation.y = self.grasp_pose_in_world.pose.position.y
        approach_tf.transform.translation.z = self.grasp_pose_in_world.pose.position.z + 0.15
        approach_tf.transform.rotation = self.grasp_pose_in_world.pose.orientation
        self.tf_broadcaster.sendTransform(approach_tf)

    # ------------------------------------------------------------------
    # Single task executor
    # ------------------------------------------------------------------

    def _execute_task(self, t: Dict[str, Any]) -> None:
        name = str(t.get("name", "task"))
        ttype = str(t.get("type", "")).strip()

        # --- Bag recorder: start
        if ttype == "start_bag":
            name_prefix = str(t.get("name_prefix", name))
            topics = list(t.get("topics", []))
            if not topics:
                raise RuntimeError(f"Task '{name}': 'topics' è vuoto o mancante.")
            self._active_bag_name = start_bag_recording(
                self,
                self.bag_start_client,
                name_prefix,
                topics,
                wait_for_server_sec=self.wait_for_server_sec,
            )
            return

        # --- Bag recorder: stop
        if ttype == "stop_bag":
            if self._active_bag_name is None:
                raise RuntimeError(
                    f"Task '{name}': nessuna bag attiva da fermare. "
                    "Hai eseguito un task 'start_bag' prima?"
                )
            stop_bag_recording(
                self,
                self.bag_stop_client,
                self._active_bag_name,
                discard=bool(t.get("discard", False)),
                description=str(t.get("description", "")),
                wait_for_server_sec=self.wait_for_server_sec,
            )
            self._active_bag_name = None
            return

        # --- Gripper command
        if ttype == "gripper_control/Gripper":
            cmd_pos = float(t.get("position", 0.5))
            max_effort = float(t.get("max_effort", 0.5))
            required_stall = bool(t.get("required_stall", False))

            reached, stalled, final_pos = send_gripper_goal(
                self, self.gripper_client, cmd_pos, max_effort
            )

            if required_stall and not stalled:
                raise RuntimeError(f"Stallo richiesto ma non rilevato per il task: {name}")
            if (not required_stall) and (not reached):
                raise RuntimeError(f"Comando gripper fallito per il task: {name}")

            if self._is_full_close_value(final_pos):
                raise FullCloseError(
                    f"[FULL CLOSE] (gripper result) task='{name}': final_pos={final_pos:.4f} "
                    f"(~={self.full_close_position:.4f})"
                )
            return

        # --- Motion task
        start_state = self._build_start_state_from_joint_states()

        if "goal_configuration" in t:
            goal_conf = t["goal_configuration"]
            if not isinstance(goal_conf, list):
                raise RuntimeError("goal_configuration deve essere una lista di float.")
            mpr = build_motion_plan_request(
                goal_conf,
                start_state,
                group_name=self.group_name,
                joint_names=self.joint_names,
                num_planning_attempts=self.num_planning_attempts,
                allowed_planning_time=self.allowed_planning_time,
                max_vel=self.max_vel,
                max_acc=self.max_acc,
                joint_tolerance=self.joint_tolerance,
            )

        elif "cartesian_goal" in t:
            cartesian_goal = t["cartesian_goal"]
            if not isinstance(cartesian_goal, dict):
                raise RuntimeError("cartesian_goal deve essere un dizionario.")
            mpr = build_motion_plan_request_from_cartesian_goal(
                cartesian_goal,
                start_state,
                group_name=self.group_name,
                num_planning_attempts=self.num_planning_attempts,
                allowed_planning_time=self.allowed_planning_time,
                max_vel=self.max_vel,
                max_acc=self.max_acc,
            )

        else:
            raise RuntimeError(f"Task '{name}': nessun goal valido trovato.")

        geom = t.get("geometric_constraints", []) or []
        if not isinstance(geom, list):
            raise RuntimeError("geometric_constraints deve essere una lista.")
        gca = build_geometric_constraints_array(geom, stamp=self.get_clock().now().to_msg())

        plan_and_execute(
            self,
            self.plan_client,
            self.tp_client,
            self.exec_client,
            mpr,
            gca,
            plan_action_name=self.plan_action_name,
            time_param_action_name=self.time_param_action_name,
            execute_action_name=self.execute_action_name,
            max_vel=self.max_vel,
            max_acc=self.max_acc,
            cartesian_speed_limited_link=self.cartesian_speed_limited_link,
            max_cartesian_speed=self.max_cartesian_speed,
            dry_run=self.dry_run,
            verbose=self.verbose,
        )

        # Full-close check after movement (only inside the suffix)
        if self._in_candidate_suffix:
            self._raise_if_gripper_full_close_from_joint_states(where=f"after movement '{name}'")

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def _recovery_open_and_return(self, suffix_tasks: List[Dict[str, Any]]) -> None:
        # 1) Open gripper
        try:
            send_gripper_goal(self, self.gripper_client, position=0.0, max_effort=50.0)
        except Exception:
            pass

        # 2) Execute the 'return' task if present
        try:
            return_task = next(
                (tt for tt in suffix_tasks if str(tt.get("name", "")).lower() == "return"),
                None,
            )
            if return_task is not None:
                old = self._in_candidate_suffix
                self._in_candidate_suffix = False
                try:
                    self._execute_task(return_task)
                finally:
                    self._in_candidate_suffix = old
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        self.get_logger().info(f"Attendo JointState su: {self.joint_states_topic}")
        self._wait_for_joint_state()
        self.get_logger().info("JointState ricevuto. Avvio pipeline.")
        self._wait_servers()

        tasks = self._load_tasks()
        self.get_logger().info(f"Caricati {len(tasks)} task da: {self.tasks_yaml_path}")

        # Split tasks into prefix / grasp / suffix
        grasp_idx = next(
            (i for i, t in enumerate(tasks) if str(t.get("type", "")).strip() == "grasp_detection/GetGrasps"),
            None,
        )
        if grasp_idx is None:
            raise RuntimeError("Nel YAML non esiste un task di tipo 'grasp_detection/GetGrasps'.")

        prefix_tasks = tasks[:grasp_idx]
        suffix_tasks = tasks[grasp_idx + 1:]

        cycle_idx = 0
        recompute_attempts = 0

        while rclpy.ok():
            if recompute_attempts >= self.max_recompute_attempts:
                self.get_logger().error(
                    f"Raggiunto max_recompute_attempts={self.max_recompute_attempts}. Interrompo per sicurezza."
                )
                break

            self.get_logger().info("=======================================")
            self.get_logger().info(f"Ciclo pick&place (oggetto) #{cycle_idx + 1}")
            self.get_logger().info("=======================================")

            # PREFIX
            try:
                self._in_candidate_suffix = False
                for t in prefix_tasks:
                    self.get_logger().info(f"[PREFIX] {t.get('name', 'task')}")
                    self._execute_task(t)
            except Exception as e:
                self.get_logger().error(f"Errore PREFIX: {e}")
                if self.stop_on_error:
                    raise
                break

            # PERCEPTION — retry on empty or low-score
            candidates: List[Tuple[PoseStamped, PoseStamped, float]] = []
            for attempt in range(1, self.empty_grasp_retries + 1):
                candidates = call_get_grasps(
                    self,
                    self.grasp_client,
                    self.tf_buffer,
                    self.grasp_pose_publisher,
                    top_k=20,
                    wait_for_server_sec=self.wait_for_server_sec,
                )

                if not candidates:
                    self.get_logger().warn(
                        f"Nessuna grasp (tentativo {attempt}/{self.empty_grasp_retries}). Riprovo..."
                    )
                    time.sleep(self.empty_grasp_retry_delay_sec)
                    continue

                best_score = float(candidates[0][2])
                if best_score < self.min_grasp_score:
                    self.get_logger().warn(
                        f"Best score troppo basso ({best_score:.3f} < {self.min_grasp_score:.3f}) "
                        f"(tentativo {attempt}/{self.empty_grasp_retries}). Riprovo..."
                    )
                    candidates = []
                    time.sleep(self.empty_grasp_retry_delay_sec)
                    continue

                break  # valid grasp found

            if not candidates:
                self.get_logger().info("Nessuna grasp valida dopo i retry: considero scena vuota. Termino.")
                break

            # Always use the single best candidate (no fallback to next)
            ps_cam, ps_world, score = candidates[0]
            self.get_logger().info(f"Uso grasp migliore (score={score:.3f}).")
            self._set_current_grasp(ps_cam, ps_world)

            # SUFFIX
            try:
                self._in_candidate_suffix = True
                for t in suffix_tasks:
                    self.get_logger().info(f"[SUFFIX] {t.get('name', 'task')}")
                    self._execute_task(t)

                self._in_candidate_suffix = False
                recompute_attempts = 0
                cycle_idx += 1
                self.get_logger().info("Pick&place completato con successo.")

            except Exception as e:
                self._in_candidate_suffix = False
                self.get_logger().warn(f"{e} -> RECOVERY + ricalcolo grasp da zero.")
                self._recovery_open_and_return(suffix_tasks)
                recompute_attempts += 1

        self.get_logger().info(f"Pipeline completata. Pick&place eseguiti: {cycle_idx}")
