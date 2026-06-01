/*
 * MIT License
 *
 * Copyright (c) 2025 Meher V.R. Malladi.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "base_node.hpp"
#include "rko_lio/core/process_timestamps.hpp"
#include "rko_lio/core/profiler.hpp"
#include "rko_lio/ros/utils/utils.hpp"
// ros
#include <nav_msgs/msg/odometry.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
// other
#include <stdexcept>

namespace rko_lio::ros {

// Sequential variant of the LIO node: IMU and LiDAR are processed inline on
// the rclcpp executor thread. The IMU callback feeds add_imu_measurement and
// publishes IMU-rate odometry; the LiDAR callback runs register_scan inline
// and publishes the lidar-rate odometry, deskewed scan, and TF (by default).
// Force `odom_at_imu_rate` on for this node specifically by injecting a parameter override.
// BaseNode reads that flag in its constructor and creates `odom_at_imu_rate_publisher` accordingly.
static rclcpp::NodeOptions force_imu_rate_on(const rclcpp::NodeOptions& options) {
  auto merged = options;
  auto overrides = merged.parameter_overrides();
  overrides.emplace_back("odom_at_imu_rate", true);
  merged.parameter_overrides(overrides);
  return merged;
}

class OnlineImuRateNode : public BaseNode {
public:
  rclcpp::Subscription<sensor_msgs::msg::Imu>::SharedPtr imu_sub;
  rclcpp::Subscription<sensor_msgs::msg::PointCloud2>::SharedPtr lidar_sub;
  core::Timer timer;

  // Streaming-only TF behaviour: optionally republish the odom->base TF at every IMU sample
  // (in addition to the LiDAR-rate TF). Defaults to off because flooding /tf is rarely useful.
  bool tf_at_imu_rate = false;

  OnlineImuRateNode(const OnlineImuRateNode&) = delete;
  OnlineImuRateNode(OnlineImuRateNode&&) = delete;
  OnlineImuRateNode& operator=(const OnlineImuRateNode&) = delete;
  OnlineImuRateNode& operator=(OnlineImuRateNode&&) = delete;

  explicit OnlineImuRateNode(const rclcpp::NodeOptions& options)
      : BaseNode("rko_lio_online_imu_rate_node", force_imu_rate_on(options)),
        timer("RKO LIO Online IMU-rate Node") {
    tf_at_imu_rate = node->declare_parameter<bool>("seq.tf_at_imu_rate", tf_at_imu_rate);

    RCLCPP_INFO_STREAM(node->get_logger(),
                       "OnlineImuRateNode publishing IMU-rate odometry to "
                           << odom_at_imu_rate_topic << ", TF rate: " << (tf_at_imu_rate ? "imu" : "lidar"));

    const auto qos_imu = rclcpp::SensorDataQoS().keep_last(100);
    const auto qos_lidar = rclcpp::SensorDataQoS().keep_last(10);

    imu_sub = node->create_subscription<sensor_msgs::msg::Imu>(
        imu_topic, qos_imu, [this](const sensor_msgs::msg::Imu::ConstSharedPtr& imu_msg) { imu_callback(imu_msg); });

    lidar_sub = node->create_subscription<sensor_msgs::msg::PointCloud2>(
        lidar_topic, qos_lidar,
        [this](const sensor_msgs::msg::PointCloud2::ConstSharedPtr& lidar_msg) { lidar_callback(lidar_msg); });
  }

  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr get_node_base_interface() {
    return node->get_node_base_interface();
  }

  void imu_callback(const sensor_msgs::msg::Imu::ConstSharedPtr& imu_msg) {
    if (!ensure_frame_and_extrinsics(imu_frame, imu_msg->header.frame_id, "IMU")) {
      return;
    }

    const core::ImuControl imu_data = imu_msg_to_imu_data(*imu_msg);
    lio->add_imu_measurement(extrinsic_imu2base, imu_data);

    if (!(lio->imu_state.time > core::Nsec{0})) {
      // Skip publishing before the first successful registration: until then,
      // imu_state has not been seeded from a real lidar pose. add_imu_measurement
      // leaves imu_state.time at zero in that pre-init phase.
      return;
    }
    publish_odometry(lio->imu_state, odom_at_imu_rate_publisher);
    if (tf_at_imu_rate) {
      publish_tf(lio->imu_state);
    }
  }

  void lidar_callback(const sensor_msgs::msg::PointCloud2::ConstSharedPtr& lidar_msg) {
    if (!ensure_frame_and_extrinsics(lidar_frame, lidar_msg->header.frame_id, "LiDAR")) {
      return;
    }

    try {
      const auto [timestamps, scan] = process_lidar_msg(lidar_msg);
      const core::Vector3dVector deskewed_frame = register_scan_locked(scan, timestamps.times);
      if (deskewed_frame.empty()) {
        // first frame is skipped and an empty frame is returned. nothing to publish.
        return;
      }
      publish_lidar_outputs(deskewed_frame, lidar_msg);
      publish_tf(lio->lidar_state);
    } catch (const std::invalid_argument& ex) {
      RCLCPP_ERROR_STREAM(node->get_logger(), "Encountered error, dropping frame. Error: " << ex.what());
    }
  }

};

} // namespace rko_lio::ros

#include <rclcpp_components/register_node_macro.hpp>
RCLCPP_COMPONENTS_REGISTER_NODE(rko_lio::ros::OnlineImuRateNode)
