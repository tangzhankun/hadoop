package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

public class DeviceRegisterRequest {

  private String version;
  private String resourceName;

  public DeviceRegisterRequest(String version, String resourceName) {
    this.resourceName = resourceName;
    this.version = version;
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }
}
