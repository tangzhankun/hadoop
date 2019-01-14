/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.*;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class DeviceResourceDockerRuntimePluginImpl
    implements DockerCommandPlugin {

  final static Log LOG = LogFactory.getLog(
      DeviceResourceDockerRuntimePluginImpl.class);

  private String resourceName;
  private DevicePlugin devicePlugin;
  private DevicePluginAdapter devicePluginAdapter;

  private Map<ContainerId, Set<Device>> cachedAllocation =
      new ConcurrentHashMap();

  private Map<ContainerId, DeviceRuntimeSpec> cachedSpec =
      new ConcurrentHashMap<>();

  public DeviceResourceDockerRuntimePluginImpl(String resourceName,
      DevicePlugin devicePlugin, DevicePluginAdapter devicePluginAdapter) {
    this.resourceName = resourceName;
    this.devicePlugin = devicePlugin;
    this.devicePluginAdapter = devicePluginAdapter;
  }

  @Override
  public void updateDockerRunCommand(DockerRunCommand dockerRunCommand,
      Container container) throws ContainerExecutionException {
    LOG.debug("Try to update docker run command.");
    if(!requestsDevice(resourceName, container)) {
      return;
    }
    DeviceRuntimeSpec deviceRuntimeSpec = getRuntimeSpec(container);
    if (deviceRuntimeSpec == null) {
      LOG.warn("The container return null for device runtime spec");
      return;
    }
    // handle runtime
    dockerRunCommand.addRuntime(deviceRuntimeSpec.getContainerRuntime());
    LOG.debug("Handle docker container runtime type: "
        + deviceRuntimeSpec.getContainerRuntime());
    // handle device mounts
    Set<MountDeviceSpec> deviceMounts = deviceRuntimeSpec.getDeviceMounts();
    LOG.debug("Handle device mounts: " + deviceMounts);
    for (MountDeviceSpec mountDeviceSpec : deviceMounts) {
      dockerRunCommand.addDevice(
          mountDeviceSpec.getDevicePathInHost(),
          mountDeviceSpec.getDevicePathInContainer());
    }
    // handle volume mounts
    Set<MountVolumeSpec> mountVolumeSpecs = deviceRuntimeSpec.getVolumeMounts();
    LOG.debug("Handle volume mounts: " + mountVolumeSpecs);
    for (MountVolumeSpec mountVolumeSpec : mountVolumeSpecs) {
      if (mountVolumeSpec.getReadOnly()) {
        dockerRunCommand.addReadOnlyMountLocation(
            mountVolumeSpec.getHostPath(),
            mountVolumeSpec.getMountPath());
      } else {
        dockerRunCommand.addReadWriteMountLocation(
            mountVolumeSpec.getHostPath(),
            mountVolumeSpec.getMountPath());
      }
    }
    // handle envs
    dockerRunCommand.addEnv(deviceRuntimeSpec.getEnvs());
    LOG.debug("Handle envs: " + deviceRuntimeSpec.getEnvs());
  }

  @Override
  public DockerVolumeCommand getCreateDockerVolumeCommand(Container container)
      throws ContainerExecutionException {
    if(!requestsDevice(resourceName, container)) {
      return null;
    }
    DeviceRuntimeSpec deviceRuntimeSpec = getRuntimeSpec(container);
    if (deviceRuntimeSpec == null) {
      return null;
    }
    Set<VolumeSpec> volumeClaims = deviceRuntimeSpec.getVolumeSpecs();
    for (VolumeSpec volumeSec: volumeClaims) {
      if (volumeSec.getVolumeOperation().equals(VolumeSpec.CREATE)) {
        DockerVolumeCommand command = new DockerVolumeCommand(
            DockerVolumeCommand.VOLUME_CREATE_SUB_COMMAND);
        command.setDriverName(volumeSec.getVolumeDriver());
        command.setVolumeName(volumeSec.getVolumeName());
        LOG.debug("Get volume create request from plugin:" + volumeClaims);
        return command;
      }
    }
    return null;
  }

  @Override
  public DockerVolumeCommand getCleanupDockerVolumesCommand(Container container)
      throws ContainerExecutionException {

    if(!requestsDevice(resourceName, container)) {
      return null;
    }
    Set<Device> allocated = new TreeSet<>();
    getAllocatedDevices(container, allocated);
    try {
      devicePlugin.onDevicesReleased(allocated);
    } catch (Exception e) {
      LOG.warn("Exception thrown onDeviceReleased of " + devicePlugin.getClass()
          + e.getMessage());
    }
    // remove cache
    ContainerId containerId = container.getContainerId();
    cachedAllocation.remove(containerId);
    cachedSpec.remove(containerId);
    return null;
  }

  protected boolean requestsDevice(String resourceName, Container container) {
    return DeviceMappingManager.
        getRequestedDeviceCount(resourceName, container.getResource()) > 0;
  }

  private void getAllocatedDevices(Container container, Set<Device> allocated) {
    // get allocated devices
    ContainerId containerId = container.getContainerId();
    allocated = cachedAllocation.get(containerId);
    if (null == allocated) {
      return;
    }
    Map<Device, ContainerId> assignedDevice = devicePluginAdapter
        .getDeviceMappingManager()
        .getAllUsedDevices().get(resourceName);
    for (Map.Entry<Device, ContainerId> entry : assignedDevice.entrySet()) {
      if (entry.getValue().equals(containerId)) {
        allocated.add(entry.getKey());
      }
    }
    cachedAllocation.put(containerId, allocated);
  }

  public synchronized DeviceRuntimeSpec getRuntimeSpec(Container container) {
    ContainerId containerId = container.getContainerId();
    DeviceRuntimeSpec deviceRuntimeSpec = cachedSpec.get(containerId);
    if (deviceRuntimeSpec == null) {
      Set<Device> allocated = new TreeSet<>();
      getAllocatedDevices(container, allocated);
      try {
        deviceRuntimeSpec = devicePlugin.onDevicesAllocated(allocated,
            YarnRuntimeType.RUNTIME_DOCKER);
      } catch (Exception e) {
        LOG.error("Exception thrown in onDeviceAllocated of "
            + devicePlugin.getClass() + e.getMessage());
      }
      if (null == deviceRuntimeSpec) {
        LOG.error("Null DeviceRuntimeSpec value got," +
            " please check plugin logic");
        return null;
      }
      cachedSpec.put(containerId, deviceRuntimeSpec);
    }
    LOG.debug("Get runtime spec from plugin:" + deviceRuntimeSpec);
    return deviceRuntimeSpec;
  }

}
