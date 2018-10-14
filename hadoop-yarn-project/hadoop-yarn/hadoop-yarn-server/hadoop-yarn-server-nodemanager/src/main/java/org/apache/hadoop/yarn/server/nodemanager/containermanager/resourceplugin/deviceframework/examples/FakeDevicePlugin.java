package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.examples;

import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.*;

import java.util.Set;
import java.util.TreeSet;

public class FakeDevicePlugin implements DevicePlugin {
  @Override
  public DeviceRegisterRequest register() {
    return new DeviceRegisterRequest(DeviceConstants.version, "cmp.com/cmp");
  }

  @Override
  public Set<Device> getAndWatch() {
    TreeSet<Device> r = new TreeSet<>();
    r.add(new Device(0, "/dev/cmp0",
        243, 0,
        "0000:65:00.0", true));
    return r;
  }

  @Override
  public DeviceRuntimeSpec preLaunchContainer(Set<Device> allocatedDevices) {
    return null;
  }

  @Override
  public void postCompleteContainer(Set<Device> allocatedDevices) {

  }
}
