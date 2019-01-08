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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * The Hooks into container lifecycle.
 * Get device list from device plugin in {@code bootstrap}
 * Assign devices for a container in {@code preStart}
 * Restore statue in {@code reacquireContainer}
 * Recycle devices from container in {@code postComplete}
 * */
public class DeviceResourceHandlerImpl implements ResourceHandler {

  static final Log LOG = LogFactory.getLog(DeviceResourceHandlerImpl.class);

  private final String resourceName;
  private final DevicePlugin devicePlugin;
  private final DeviceMappingManager deviceMappingManager;
  private final CGroupsHandler cGroupsHandler;
  private final PrivilegedOperationExecutor privilegedOperationExecutor;
  private final DevicePluginAdapter devicePluginAdapter;
  private final Context nmContext;

  // This will be used by container-executor to add necessary clis
  public static final String EXCLUDED_DEVICES_CLI_OPTION = "--excluded_devices";
  public static final String ALLOWED_DEVICES_CLI_OPTION = "--allowed_devices";
  public static final String CONTAINER_ID_CLI_OPTION = "--container_id";

  public DeviceResourceHandlerImpl(String reseName,
      DevicePlugin devPlugin,
      DevicePluginAdapter devPluginAdapter,
      DeviceMappingManager devMappingManager,
      CGroupsHandler cgHandler,
      PrivilegedOperationExecutor operation,
      Context ctx) {
    this.devicePluginAdapter = devPluginAdapter;
    this.resourceName = reseName;
    this.devicePlugin = devPlugin;
    this.cGroupsHandler = cgHandler;
    this.privilegedOperationExecutor = operation;
    this.deviceMappingManager = devMappingManager;
    this.nmContext = ctx;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration)
      throws ResourceHandlerException {
    Set<Device> availableDevices = null;
    try {
      availableDevices = devicePlugin.getDevices();
    } catch (Exception e) {
      throw new ResourceHandlerException("Exception thrown from"
          + " plugin's \"getDevices\"" + e.getMessage());
    }
    /**
     * We won't fail the NM if plugin returns invalid value here.
     * */
    if (availableDevices == null) {
      LOG.error("Bootstrap " + resourceName
          + " failed. Null value got from plugin's getDevices method");
      return null;
    }
    // Add device set. Here we trust the plugin's return value
    deviceMappingManager.addDeviceSet(resourceName, availableDevices);
    // TODO: Init cgroups

    return null;
  }

  @Override
  public synchronized List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    DeviceMappingManager.DeviceAllocation allocation =
        deviceMappingManager.assignDevices(resourceName, container);
    LOG.debug("Allocated to "
        + containerIdStr + ": " + allocation);

    DeviceRuntimeSpec spec;
    try {
      spec = devicePlugin.onDevicesAllocated(
          allocation.getAllowed(), YarnRuntimeType.RUNTIME_DEFAULT);
    } catch (Exception e) {
      throw new ResourceHandlerException("Exception thrown from"
          + " plugin's \"onDeviceAllocated\"" + e.getMessage());
    }

    // cgroups operation based on allocation
    if (spec != null) {
      LOG.warn("Runtime spec in non-Docker container is not supported yet!");
    }
    // Create device cgroups for the container
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerIdStr);
    // non-Docker, use cgroups to do isolation
    if (!DockerLinuxContainerRuntime.isDockerContainerRequested(
        nmContext.getConf(),
        container.getLaunchContext().getEnvironment())) {
      try {
        // Execute c-e to setup device isolation before launch the container
        PrivilegedOperation privilegedOperation = new PrivilegedOperation(
            PrivilegedOperation.OperationType.DEVICE,
            Arrays.asList(CONTAINER_ID_CLI_OPTION, containerIdStr));
        if (!allocation.getDenied().isEmpty()) {
          String devType;
          int majorNumber;
          int minorNumber;
          List<String> devNumbers = new ArrayList<>();
          for (Device deniedDevice : allocation.getDenied()) {
            majorNumber = deniedDevice.getMajorNumber();
            minorNumber = deniedDevice.getMajorNumber();
            // Add device type
            devType = getDeviceType(majorNumber, minorNumber);
            devNumbers.add(devType + "-" + majorNumber + ":" + minorNumber);
          }
          privilegedOperation.appendArgs(
              Arrays.asList(EXCLUDED_DEVICES_CLI_OPTION,
              StringUtils.join(",", devNumbers)));
        }

        if (!allocation.getAllowed().isEmpty()) {
          int majorNumber;
          int minorNumber;
          List<String> devNumbers = new ArrayList<>();
          for (Device deniedDevice : allocation.getDenied()) {
            majorNumber = deniedDevice.getMajorNumber();
            minorNumber = deniedDevice.getMajorNumber();
            devNumbers.add(majorNumber + ":" + minorNumber);
          }
          privilegedOperation.appendArgs(
              Arrays.asList(ALLOWED_DEVICES_CLI_OPTION,
                  StringUtils.join(",", devNumbers)));
        }
        privilegedOperationExecutor.executePrivilegedOperation(
            privilegedOperation, true);
      } catch (PrivilegedOperationException e) {
        cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
            containerIdStr);
        LOG.warn("Could not update cgroup for container", e);
        throw new ResourceHandlerException(e);
      }

      List<PrivilegedOperation> ret = new ArrayList<>();
      ret.add(new PrivilegedOperation(
          PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
          PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupsHandler
              .getPathForCGroupTasks(CGroupsHandler.CGroupController.DEVICES,
                  containerIdStr)));

      return ret;
    }
    return null;
  }

  @Override
  public synchronized List<PrivilegedOperation> reacquireContainer(
      ContainerId containerId) throws ResourceHandlerException {
    deviceMappingManager.recoverAssignedDevices(resourceName, containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public synchronized List<PrivilegedOperation> postComplete(
      ContainerId containerId) throws ResourceHandlerException {
    deviceMappingManager.cleanupAssignedDevices(resourceName, containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown()
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public String toString() {
    return DeviceResourceHandlerImpl.class.getName() + "{" +
        "resourceName='" + resourceName + '\'' +
        ", devicePlugin=" + devicePlugin +
        ", devicePluginAdapter=" + devicePluginAdapter +
        '}';
  }

  /**
   * Get the device type used for cgroups value set.
   * If /sys/dev/char/major:minor exists,
   * */
  private String getDeviceType(int major, int minor) {
    File searchFile =
        new File("/sys/dev/char/" + major + ":" + minor);
    if (searchFile.exists()) {
      return "c";
    }
    return "b";
  }

}
