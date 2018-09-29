package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

import java.util.Set;

public interface DevicePlugin {
  DeviceRegisterRequest register();
  Set<Device> getAndWatch();
  DeviceRuntimeSpec preLaunchContainer(Set<Device> allocatedDevices);
  void postCompleteContainer(Set<Device> allocatedDevices);
}
