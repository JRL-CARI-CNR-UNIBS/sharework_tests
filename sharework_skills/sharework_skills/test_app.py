#!/usr/bin/env python3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import rclpy
from rclpy.node import Node
from rclpy.action import ActionClient
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy
import yaml

from sensor_msgs.msg import JointState

# Actions
from pose_constraints_msgs.action import PlanWithConstraints
from time_parametrization_msgs.action import ApplyTimeParametrization
from moveit_msgs.action import ExecuteTrajectory
from control_msgs.action import GripperCommand

# Messages
from pose_constraints_msgs.msg import GeometricConstraintArray, GeometricConstraint
from moveit_msgs.msg import (
    MotionPlanRequest,
    MotionPlanResponse,
    Constraints,
    JointConstraint,
    MoveItErrorCodes,
    RobotState,
)


def _set_xyz(obj: Any, xyz: Any, field_name: str) -> None:
    if xyz is None:
        return
    if not (isinstance(xyz, (list, tuple)) and len(xyz) == 3):
        raise RuntimeError(f"Campo '{field_name}' atteso come lista [x,y,z], ricevuto: {xyz}")
    if not (hasattr(obj, "x") and hasattr(obj, "y") and hasattr(obj, "z")):
        raise RuntimeError(f"Campo '{field_name}' non ha attributi x/y/z (tipo inatteso).")
    obj.x = float(xyz[0])
    obj.y = float(xyz[1])
    obj.z = float(xyz[2])


def _is_success(error_code_msg: Any) -> bool:
    try:
        return int(error_code_msg.val) == int(MoveItErrorCodes.SUCCESS)
    except Exception:
        return False


class PoseConstraintsPipelineNode(Node):
    def __init__(self) -> None:
        super().__init__("pose_constraints_pipeline")

        # ---- Parameters
        self.declare_parameter("tasks_yaml_path", "")
        self.declare_parameter("group_name", "")

        # IMPORTANT: default non-vuoto per forzare STRING_ARRAY
        self.declare_parameter("joint_names", [""])

        self.declare_parameter("joint_states_topic", "/joint_states")
        self.declare_parameter("wait_joint_states_sec", 5.0)
        self.declare_parameter("max_joint_state_age_sec", 1.0)  # warning se troppo vecchio

        self.declare_parameter("joint_tolerance", 0.001)
        self.declare_parameter("num_planning_attempts", 1)
        self.declare_parameter("allowed_planning_time", 15.0)

        # Scaling factors used by BOTH planning request and time parametrization action goal
        self.declare_parameter("max_velocity_scaling_factor", 0.1)
        self.declare_parameter("max_acceleration_scaling_factor", 0.1)

        # Cartesian speed limiting parameters for the NEW ApplyTimeParametrization action
        self.declare_parameter("cartesian_speed_limited_link", "")
        self.declare_parameter("max_cartesian_speed", 0.25)  # m/s ; <=0 => ignored

        self.declare_parameter("verbose", False)
        self.declare_parameter("stop_on_error", True)
        self.declare_parameter("dry_run", False)

        self.declare_parameter("plan_action_name", "/plan_with_constraints")
        self.declare_parameter("time_param_action_name", "/apply_time_parametrization")
        self.declare_parameter("execute_action_name", "/execute_trajectory")
        self.declare_parameter("wait_for_server_sec", 10.0)

        # ---- Read params
        self.tasks_yaml_path = str(self.get_parameter("tasks_yaml_path").value)

        self.group_name = str(self.get_parameter("group_name").value)
        print("group_name =", self.group_name)

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

        # ---- Validate
        if not self.tasks_yaml_path:
            raise RuntimeError("Parametro 'tasks_yaml_path' vuoto.")
        if not self.group_name:
            raise RuntimeError("Parametro 'group_name' vuoto.")
        if not self.joint_names:
            raise RuntimeError("Parametro 'joint_names' vuoto.")

        # ---- JointState cache
        self._latest_js: Optional[JointState] = None
        self._latest_js_rx_time = None  # rclpy Time

        qos = QoSProfile(depth=10)
        qos.reliability = ReliabilityPolicy.BEST_EFFORT
        qos.durability = DurabilityPolicy.VOLATILE

        self._js_sub = self.create_subscription(
            JointState,
            self.joint_states_topic,
            self._on_joint_state,
            qos,
        )

        # ---- Action clients
        self.plan_client = ActionClient(self, PlanWithConstraints, self.plan_action_name)
        self.tp_client = ActionClient(self, ApplyTimeParametrization, self.time_param_action_name)
        self.exec_client = ActionClient(self, ExecuteTrajectory, self.execute_action_name)
        self.gripper_client = ActionClient(self, GripperCommand, "/robotiq_action_controller/gripper_cmd")

    def _on_joint_state(self, msg: JointState) -> None:
        self._latest_js = msg
        self._latest_js_rx_time = self.get_clock().now()

    def _wait_for_joint_state(self) -> None:
        deadline = self.get_clock().now() + rclpy.duration.Duration(seconds=self.wait_joint_states_sec)

        while rclpy.ok() and self._latest_js is None and self.get_clock().now() < deadline:
            rclpy.spin_once(self, timeout_sec=0.1)

        if self._latest_js is None:
            raise RuntimeError(
                f"Non ho ricevuto alcun JointState su '{self.joint_states_topic}' entro {self.wait_joint_states_sec}s."
            )

    def _build_start_state_from_joint_states(self) -> RobotState:
        """
        Costruisce RobotState.start_state coerente con joint_names (ordine!).
        - Richiede che _latest_js sia presente.
        - Se manca un giunto richiesto, alza errore.
        """
        if self._latest_js is None:
            raise RuntimeError("JointState non disponibile (cache vuota).")

        js = self._latest_js

        # freshness warning
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
            raise RuntimeError(
                "JointState non contiene tutti i giunti richiesti (o posizioni mancanti). "
                f"Mancanti: {missing}. "
                f"Ricevuti: {list(js.name)}"
            )

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

    def _wait_servers(self) -> None:
        for client, name in [
            (self.plan_client, self.plan_action_name),
            (self.tp_client, self.time_param_action_name),
            (self.exec_client, self.execute_action_name),
            (self.gripper_client, "/robotiq_action_controller/gripper_cmd"),
        ]:
            self.get_logger().info(f"Attendo action server: {name}")
            if not client.wait_for_server(timeout_sec=self.wait_for_server_sec):
                raise RuntimeError(f"Action server non disponibile entro {self.wait_for_server_sec}s: {name}")

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

    def _yaml_type_to_uint8(self, t: Any) -> int:
        if isinstance(t, int):
            return int(t)

        s = str(t).strip().lower()
        if s == "plane":
            return int(GeometricConstraint.PLANE)
        if s == "line":
            return int(GeometricConstraint.LINE)
        if s in ("angle", "orientation", "orient"):
            return int(GeometricConstraint.ANGLE)
        if s.isdigit():
            return int(s)

        raise RuntimeError(f"Tipo vincolo non supportato: '{t}'. Attesi: plane/line/orientation (o 0/1/2).")

    def _build_geometric_constraints_array(self, geometric_constraints: List[Dict[str, Any]]) -> GeometricConstraintArray:
        arr = GeometricConstraintArray()
        try:
            arr.header.stamp = self.get_clock().now().to_msg()
        except Exception:
            pass

        for item in geometric_constraints:
            if not isinstance(item, dict):
                continue

            gc = GeometricConstraint()
            gc.name = str(item.get("name", ""))
            gc.frame_id = str(item.get("frame", item.get("frame_id", "")))
            gc.type = int(self._yaml_type_to_uint8(item.get("type", "")))

            if gc.type == GeometricConstraint.PLANE:
                _set_xyz(gc.plane_origin, item.get("origin", None), "origin")
                _set_xyz(gc.plane_normal, item.get("normal", None), "normal")
                tol = item.get("plane_tolerance", item.get("tolerance", 0.0))
                gc.plane_tolerance = float(tol)

            elif gc.type == GeometricConstraint.LINE:
                _set_xyz(gc.line_origin, item.get("origin", None), "origin")
                _set_xyz(gc.line_direction, item.get("direction", None), "direction")
                gc.line_max_distance = float(item.get("max_distance", item.get("line_max_distance", 0.0)))

            elif gc.type == GeometricConstraint.ANGLE:
                _set_xyz(gc.max_angle, item.get("max_angle", None), "max_angle")

            else:
                raise RuntimeError(f"GeometricConstraint.type non gestito: {gc.type}")

            arr.constraints.append(gc)

        return arr

    def _build_motion_plan_request(self, goal_configuration: List[float], start_state: RobotState) -> MotionPlanRequest:
        if len(goal_configuration) != len(self.joint_names):
            raise RuntimeError(
                f"goal_configuration ha {len(goal_configuration)} elementi, ma joint_names ne ha {len(self.joint_names)}."
            )

        req = MotionPlanRequest()
        req.group_name = self.group_name
        req.num_planning_attempts = self.num_planning_attempts
        req.allowed_planning_time = self.allowed_planning_time

        # For planning (MoveIt) this scales inside the pipeline too
        req.max_velocity_scaling_factor = self.max_vel
        req.max_acceleration_scaling_factor = self.max_acc

        req.start_state = start_state

        c = Constraints()
        c.name = "joint_goal"
        for jn, pos in zip(self.joint_names, goal_configuration):
            jc = JointConstraint()
            jc.joint_name = str(jn)
            jc.position = float(pos)
            jc.tolerance_above = self.joint_tolerance
            jc.tolerance_below = self.joint_tolerance
            jc.weight = 1.0
            c.joint_constraints.append(jc)
        req.goal_constraints.append(c)

        return req

    def _send_goal_and_wait(self, client: ActionClient, goal_msg: Any, label: str) -> Any:
        goal_future = client.send_goal_async(goal_msg)
        rclpy.spin_until_future_complete(self, goal_future)
        goal_handle = goal_future.result()

        if goal_handle is None or not goal_handle.accepted:
            raise RuntimeError(f"Goal rifiutato da action '{label}'")

        result_future = goal_handle.get_result_async()
        rclpy.spin_until_future_complete(self, result_future)
        return result_future.result().result

    def _send_gripper_goal(self, position: float, max_effort: float) -> (bool, bool):
        goal_msg = GripperCommand.Goal()
        goal_msg.command.position = float(position)
        goal_msg.command.max_effort = float(max_effort)

        self.get_logger().info(f"Invio comando gripper: position={position}, max_effort={max_effort}")

        goal_future = self.gripper_client.send_goal_async(goal_msg)
        rclpy.spin_until_future_complete(self, goal_future)
        goal_handle = goal_future.result()

        if not goal_handle.accepted:
            self.get_logger().error("Goal gripper rifiutato.")
            return False, False

        result_future = goal_handle.get_result_async()
        rclpy.spin_until_future_complete(self, result_future)
        result = result_future.result().result

        if not result:
            self.get_logger().error("Risultato gripper non disponibile.")
            return False, False

        self.get_logger().info(
            f"Comando gripper completato: position={result.position}, "
            f"reached={result.reached_goal}, stalled={result.stalled}"
        )
        return result.reached_goal, result.stalled

    def run(self) -> None:
        self.get_logger().info(f"Attendo JointState su: {self.joint_states_topic}")
        self._wait_for_joint_state()
        self.get_logger().info("JointState ricevuto. Avvio pipeline.")

        self._wait_servers()

        tasks = self._load_tasks()
        self.get_logger().info(f"Caricati {len(tasks)} task da: {self.tasks_yaml_path}")

        for idx, t in enumerate(tasks, start=1):
            name = str(t.get("name", f"task_{idx}"))
            self.get_logger().info(f"[{idx}/{len(tasks)}] Task: {name}")

            if t.get("type", "") == "gripper_control/Gripper":
                position = float(t.get("position", 0.5))
                max_effort = float(t.get("max_effort", 0.5))
                required_stall = bool(t.get("required_stall", False))

                success, stalled = self._send_gripper_goal(position, max_effort)


                if required_stall and not stalled:
                    self.get_logger().error(f"Stallo richiesto ma non rilevato per il task: {name}")
                    if self.stop_on_error:
                        raise RuntimeError(f"Stallo richiesto ma non rilevato per il task: {name}")
                elif not success:
                    self.get_logger().error(f"Comando gripper fallito per il task: {name}")
                    if self.stop_on_error:
                        raise RuntimeError(f"Comando gripper fallito per il task: {name}")


                continue


            if t.get("type", "") ==  "grasp_detection/GetGrasps":
                self.get_logger().info("Chiamata al servizio di rilevamento grasp (simulazione).")
                continue


            try:
                goal_conf = t["goal_configuration"]
                if not isinstance(goal_conf, list):
                    raise RuntimeError("goal_configuration deve essere una lista di float.")

                geom = t.get("geometric_constraints", []) or []
                if not isinstance(geom, list):
                    raise RuntimeError("geometric_constraints deve essere una lista.")

                # start_state “fresh” per ogni task
                start_state = self._build_start_state_from_joint_states()

                # ---- Build request + constraints
                mpr = self._build_motion_plan_request(goal_conf, start_state)
                gca = self._build_geometric_constraints_array(geom)

                # ---- 1) Plan with constraints
                plan_goal = PlanWithConstraints.Goal()
                plan_goal.motion_plan_request = mpr
                plan_goal.constraints = gca
                plan_goal.verbose = self.verbose

                plan_res = self._send_goal_and_wait(self.plan_client, plan_goal, self.plan_action_name)
                plan_out: MotionPlanResponse = plan_res.motion_plan_response

                if not _is_success(plan_out.error_code):
                    raise RuntimeError(f"Planning fallito (error_code={int(plan_out.error_code.val)})")

                # ---- 2) Apply time parametrization (NEW action API)
                tp_goal = ApplyTimeParametrization.Goal()
                tp_goal.unparametrized_motion_plan_response = plan_out
                tp_goal.max_velocity_scaling_factor = float(self.max_vel)
                tp_goal.max_acceleration_scaling_factor = float(self.max_acc)

                tp_goal.cartesian_speed_limited_link = str(self.cartesian_speed_limited_link)
                tp_goal.max_cartesian_speed = float(self.max_cartesian_speed)

                tp_res = self._send_goal_and_wait(self.tp_client, tp_goal, self.time_param_action_name)
                tp_out: MotionPlanResponse = tp_res.motion_plan_response

                if not _is_success(tp_out.error_code):
                    raise RuntimeError(f"Time parametrization fallita (error_code={int(tp_out.error_code.val)})")

                # ---- 3) Execute trajectory
                if self.dry_run:
                    self.get_logger().info("dry_run=True: salto execute.")
                    continue

                exec_goal = ExecuteTrajectory.Goal()
                exec_goal.trajectory = tp_out.trajectory
                exec_res = self._send_goal_and_wait(self.exec_client, exec_goal, self.execute_action_name)

                try:
                    if not _is_success(exec_res.error_code):
                        raise RuntimeError(f"Esecuzione fallita (error_code={int(exec_res.error_code.val)})")
                except Exception:
                    pass

                self.get_logger().info(f"Task '{name}' completato.")

            except Exception as e:
                self.get_logger().error(f"Task '{name}' FALLITO: {e}")
                if self.stop_on_error:
                    raise

        self.get_logger().info("Pipeline completata.")


def main(argv=None) -> None:
    rclpy.init(args=argv)
    node = None
    try:
        node = PoseConstraintsPipelineNode()
        node.run()
    finally:
        if node is not None:
            node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main(sys.argv)

