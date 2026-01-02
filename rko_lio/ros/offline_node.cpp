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

#include "node.hpp"
#include "rko_lio/core/profiler.hpp"
#include "rko_lio/ros/utils/rosbag.hpp"
#include "rko_lio/ros/utils/utils.hpp"
// other
#include <mutex>
#include <queue>
#include <rosgraph_msgs/msg/clock.hpp>
#include <sensor_msgs/msg/nav_sat_fix.hpp>
#include <std_msgs/msg/float32_multi_array.hpp>

namespace {
template <typename T>
std::shared_ptr<T> deserialize_next_msg(const rclcpp::SerializedMessage& serialized_msg) {
  const auto msg = std::make_shared<T>();
  const rclcpp::Serialization<T> serializer;
  serializer.deserialize_message(&serialized_msg, msg.get());
  return msg;
}

using BagProgressPublisher = rclcpp::Publisher<std_msgs::msg::Float32MultiArray>;
void publish_bag_progress(const BagProgressPublisher::SharedPtr& publisher,
                          const size_t processed_bag_msgs,
                          const size_t total_bag_msgs) {
  if (total_bag_msgs == 0) {
    return;
  }
  static const auto start_time = std::chrono::steady_clock::now();
  const auto now = std::chrono::steady_clock::now();
  const float elapsed_seconds = std::chrono::duration<float>(now - start_time).count();

  const float percent_complete = 100.0F * processed_bag_msgs / total_bag_msgs;
  const float avg_time_per_msg = (processed_bag_msgs > 0) ? elapsed_seconds / processed_bag_msgs : 0.0F;
  const float seconds_remaining = avg_time_per_msg * (total_bag_msgs - processed_bag_msgs);

  std_msgs::msg::Float32MultiArray progress_msg;
  progress_msg.data.resize(2);
  progress_msg.data[0] = percent_complete;
  progress_msg.data[1] = seconds_remaining;

  publisher->publish(progress_msg);
}
} // namespace

namespace rko_lio::ros {
class OfflineNode : public Node {
public:
  std::unique_ptr<utils::BufferableBag> bag;

  BagProgressPublisher::SharedPtr bag_progress_publisher;
  rclcpp::Publisher<sensor_msgs::msg::NavSatFix>::SharedPtr gps_publisher;
  bool publish_clock = false;
  size_t clock_publish_count = 0;

  float total_bag_msgs = 0;
  float processed_bag_msgs = 0;
  std::string gps_topic = "/rko_lio/gps_fix";

  // GPS message buffer to synchronize with odometry timestamps
  struct BufferedGPSMessage {
    sensor_msgs::msg::NavSatFix msg;
    core::Secondsd timestamp;
  };
  std::queue<BufferedGPSMessage> gps_buffer;
  std::mutex gps_buffer_mutex;

  // Publish GPS messages that are close to the current odometry timestamp
  // Preserves original GPS timestamps, only changes publish order for synchronization
  void publish_synchronized_gps(const core::Secondsd& current_stamp) {
    constexpr double gps_sync_tolerance = 0.5; // 500ms tolerance
    std::lock_guard<std::mutex> lock(gps_buffer_mutex);
    while (!gps_buffer.empty()) {
      const auto& buffered = gps_buffer.front();
      const double time_diff = std::chrono::abs(current_stamp - buffered.timestamp).count();

      // If GPS timestamp is close to current odometry timestamp, publish it
      // Keep original GPS timestamp - only change publish order
      if (time_diff <= gps_sync_tolerance) {
        gps_publisher->publish(buffered.msg);
        gps_buffer.pop();
      } else if (buffered.timestamp > current_stamp) {
        // GPS message is in the future, wait for odometry to catch up
        break;
      } else {
        // GPS message is too old, skip it
        gps_buffer.pop();
      }
    }
  }

  explicit OfflineNode(const rclcpp::NodeOptions& options) : Node("rko_lio_offline_node", options) {
    // increase the lidar buffer limit because we're offline
    max_lidar_buffer_size = 100;
    // bag reading - include GPS topic to republish it
    const tf2::Duration skip_to_time = tf2::durationFromSec(node->declare_parameter<double>("skip_to_time", 0.0));
    gps_topic = node->declare_parameter<std::string>("gps_topic", gps_topic);
    std::vector<std::string> bag_topics = {imu_topic, lidar_topic, gps_topic};
    bag = std::make_unique<utils::BufferableBag>(node->declare_parameter<std::string>("bag_path"),
                                                 std::make_shared<utils::BufferableBag::TFBridge>(node), bag_topics,
                                                 skip_to_time);
    total_bag_msgs = bag->message_count();
    bag_progress_publisher = node->create_publisher<std_msgs::msg::Float32MultiArray>("/rko_lio/bag_progress", 10);

    // GPS publisher to republish GPS messages from the bag
    const rclcpp::QoS publisher_qos((rclcpp::SystemDefaultsQoS().keep_last(1).durability_volatile()));
    gps_publisher = node->create_publisher<sensor_msgs::msg::NavSatFix>(gps_topic, publisher_qos);

    // Clock publishing for sim time support (enables --use-sim-time on bag recorders)
    // Clock will be published when frame/odometry messages are published to ensure synchronization
    publish_clock = node->declare_parameter<bool>("publish_clock", false);
    RCLCPP_INFO(node->get_logger(), "publish_clock parameter: %s", publish_clock ? "true" : "false");
    if (publish_clock) {
      clock_publisher = node->create_publisher<rosgraph_msgs::msg::Clock>("/clock", 10);
      RCLCPP_INFO(node->get_logger(), "Clock publishing enabled on /clock (synchronized with output messages)");
    } else {
      RCLCPP_WARN(node->get_logger(), "Clock publishing is DISABLED. Set publish_clock:=true to enable.");
    }
  }

  void run() {
    while (rclcpp::ok() && !bag->finished()) {
      {
        if (lidar_buffer.size() >= 0.9 * max_lidar_buffer_size) {
          RCLCPP_WARN_STREAM_ONCE(node->get_logger(),
                                  "Lidar buffer size: " << lidar_buffer.size()
                                                        << ", max_lidar_buffer_size: " << max_lidar_buffer_size
                                                        << ", throttling the bag reading thread as it's too fast.\n");
          // this is a hack. can be improved
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
          continue;
        }
      }
      const rosbag2_storage::SerializedBagMessage serialized_bag_msg = bag->PopNextMessage();
      const auto& topic_name = serialized_bag_msg.topic_name;
      const rclcpp::SerializedMessage serialized_msg(*serialized_bag_msg.serialized_data);

      // check the topic and call the appropriate callback
      if (topic_name == imu_topic) {
        const auto& imu_msg = deserialize_next_msg<sensor_msgs::msg::Imu>(serialized_msg);
        imu_callback(imu_msg);
        // Note: Clock is now published when frame/odometry messages are published
        // to ensure synchronization with output message timestamps
      } else if (topic_name == lidar_topic) {
        const auto& lidar_msg = deserialize_next_msg<sensor_msgs::msg::PointCloud2>(serialized_msg);
        lidar_callback(lidar_msg);
        // Note: Clock is now published when frame/odometry messages are published
        // to ensure synchronization with output message timestamps
      } else if (topic_name == gps_topic) {
        // Buffer GPS messages to synchronize with odometry timestamps
        const auto& gps_msg = deserialize_next_msg<sensor_msgs::msg::NavSatFix>(serialized_msg);
        const auto gps_stamp = utils::ros_time_to_seconds(gps_msg->header.stamp);
        std::lock_guard<std::mutex> lock(gps_buffer_mutex);
        gps_buffer.push({*gps_msg, gps_stamp});
      }

      processed_bag_msgs++;
      publish_bag_progress(bag_progress_publisher, processed_bag_msgs, total_bag_msgs);
      // Spin to ensure publishers send messages
      rclcpp::spin_some(node);
    }
    while (rclcpp::ok()) {
      {
        // even if the bag finishes, we need to wait on the registration buffers to empty
        std::lock_guard<std::mutex> lock(buffer_mutex);
        if (imu_buffer.empty() && lidar_buffer.empty()) {
          break;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (publish_clock) {
      RCLCPP_INFO(node->get_logger(), "Clock publishing was enabled (published with output messages)");
    }
  }

  // Override publish_odometry to also publish synchronized GPS messages
  void publish_odometry(const core::State& state, const core::Secondsd& stamp) const override {
    // Call base class implementation
    Node::publish_odometry(state, stamp);
    // Publish GPS messages synchronized with this odometry timestamp
    const_cast<OfflineNode*>(this)->publish_synchronized_gps(stamp);
  }
};
} // namespace rko_lio::ros

int main(int argc, char** argv) {
  const rko_lio::core::Timer timer("RKO LIO Offline Node");
  rclcpp::init(argc, argv);
  auto offline_node = rko_lio::ros::OfflineNode(rclcpp::NodeOptions());
  offline_node.run();
  rclcpp::shutdown();
  return 0;
}
