import rclpy
from rclpy.node import Node
from grasp_detection_msgs.srv import GetGrasps
from geometry_msgs.msg import PoseArray, Pose
from std_msgs.msg import Header
from time import sleep

class GraspNode(Node):

    def __init__(self):
        super().__init__('grasp_node')

        # Service client to call GetGrasps
        self.client = self.create_client(GetGrasps, '/get_grasps')
        
        # Publisher to publish PoseArray
        self.publisher = self.create_publisher(PoseArray, '/grasp_poses', 10)
        
        # Wait for the service to be available
        while not self.client.wait_for_service(timeout_sec=1.0):
            self.get_logger().info('Service not available, waiting again...')

        # Call the service in the background
        self.get_grasps()

    def get_grasps(self):
        # Create the request for the service
        request = GetGrasps.Request()

        # Make the service call
        future = self.client.call_async(request)

        # Wait for the response asynchronously
        future.add_done_callback(self.handle_response)

    def handle_response(self, future):
        # Extract the response from the future
        try:
            response = future.result()
            grasp_poses = response.poses
            grasp_scores = response.scores

            
            # Prepare PoseArray message for publishing
            pose_array = grasp_poses
            # Publish the PoseArray
            self.publisher.publish(pose_array)
            self.get_logger().info('Published PoseArray')

        except Exception as e:
            self.get_logger().error(f'Error while calling service: {e}')

def main(args=None):
    rclpy.init(args=args)
    grasp_node = GraspNode()
    rclpy.spin(grasp_node)
    grasp_node.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()