package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;


/**
 * YarnRuntime parameter enum for {@link DevicePlugin}.
 * It's passed into {@code onDevicesAllocated}.
 * Device plugin could populate {@link DeviceRuntimeSpec}
 * based on which YARN container runtime will use.
 * */
public enum YarnRuntimeType {

  RUNTIME_DEFAULT("default"),
  RUNTIME_DOCKER("docker");

  private final String name;

  YarnRuntimeType(String n) {
    this.name = n;
  }

  public String getName() {
    return name;
  }
}
