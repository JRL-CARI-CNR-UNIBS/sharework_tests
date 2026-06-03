"""
utils.py

Pure utility helpers with no ROS node dependencies.
"""

from typing import Any

from moveit_msgs.msg import MoveItErrorCodes


def set_xyz(obj: Any, xyz: Any, field_name: str) -> None:
    """Assign a [x, y, z] list/tuple to an object with .x/.y/.z attributes."""
    if xyz is None:
        return
    if not (isinstance(xyz, (list, tuple)) and len(xyz) == 3):
        raise RuntimeError(f"Campo '{field_name}' atteso come lista [x,y,z], ricevuto: {xyz}")
    if not (hasattr(obj, "x") and hasattr(obj, "y") and hasattr(obj, "z")):
        raise RuntimeError(f"Campo '{field_name}' non ha attributi x/y/z (tipo inatteso).")
    obj.x = float(xyz[0])
    obj.y = float(xyz[1])
    obj.z = float(xyz[2])


def is_success(error_code_msg: Any) -> bool:
    """Return True if a MoveIt error code represents SUCCESS."""
    try:
        return int(error_code_msg.val) == int(MoveItErrorCodes.SUCCESS)
    except Exception:
        return False
