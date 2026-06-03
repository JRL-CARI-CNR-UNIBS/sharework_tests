"""
exceptions.py

Custom exceptions for the pick & place pipeline.
"""


class FullCloseError(RuntimeError):
    """
    Raised when the gripper reaches full closure, which implies either the
    object is not in hand or the scene has changed unexpectedly.
    """
    pass
