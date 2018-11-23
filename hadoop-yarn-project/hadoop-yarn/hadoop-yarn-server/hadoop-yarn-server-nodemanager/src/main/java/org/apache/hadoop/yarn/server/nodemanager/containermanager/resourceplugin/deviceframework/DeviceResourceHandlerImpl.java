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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Set;

public class DeviceResourceHandlerImpl implements ResourceHandler {

  static final Logger LOG =
      LoggerFactory.getLogger(DeviceResourceHandlerImpl.class);

  private String resourceName;
  private DevicePlugin devicePlugin;
  private DeviceMappingManager deviceMappingManager;
  private CGroupsHandler cGroupsHandler;
  private PrivilegedOperationExecutor privilegedOperationExecutor;
  private DevicePluginAdapter devicePluginAdapter;

  private Context nmContext;

  public DeviceResourceHandlerImpl(String resourceName,
      DevicePlugin devicePlugin,
      DevicePluginAdapter devicePluginAdapter,
      DeviceMappingManager deviceMappingManager,
      CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperation,
      Context ctx) {
    this.devicePluginAdapter = devicePluginAdapter;
    this.resourceName = resourceName;
    this.devicePlugin = devicePlugin;
    this.cGroupsHandler = cGroupsHandler;
    this.privilegedOperationExecutor = privilegedOperation;
    this.deviceMappingManager = deviceMappingManager;
    this.nmContext = ctx;
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration) throws ResourceHandlerException {
    Set<Device> availableDevices = devicePlugin.getDevices();
    /**
     * We won't fail the NM if plugin returns invalid value here.
     * // TODO: we should update RM's resource count if something wrong
     * */
    if (availableDevices == null) {
      LOG.error("Bootstrap " + resourceName + " failed. Null value got from plugin's getDevices method");
      return null;
    }
    // Add device set. Here we trust the plugin's return value
    deviceMappingManager.addDeviceSet(resourceName, availableDevices);
    // TODO: Init cgroups
    // And initialize cgroups
    this.cGroupsHandler.initializeCGroupController(
        CGroupsHandler.CGroupController.DEVICES);
    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container) throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    DeviceMappingManager.DeviceAllocation allocation = deviceMappingManager.assignDevices(
        resourceName, container);
    LOG.debug("Allocated to " +
        containerIdStr + ": " + allocation );

    // cgroups operation based on allocation
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerIdStr);
    if (!DockerLinuxContainerRuntime.isDockerContainerRequested(
        nmContext.getConf(),
        container.getLaunchContext().getEnvironment())) {
      Set<Device> allowed = allocation.getAllowed();
      int major;
      int minor;
      String value;
      try {
        cGroupsHandler.updateCGroupParam(CGroupsHandler.CGroupController.DEVICES,
            containerIdStr, CGroupsHandler.CGROUP_PARAM_DEVICES_DENY, "a");
      } catch (ResourceHandlerException e) {
        LOG.error("Cannot set {} with value {} to container {}",
            CGroupsHandler.CGROUP_PARAM_DEVICES_DENY,
            "\"a\"", containerIdStr);
        cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
            containerIdStr);
        throw e;
      }
      for (Device device : allowed) {
        major = device.getMajorNumber();
        minor = device.getMinorNumber();
        String dt = getDeviceType(major, minor);
        value = dt + " " + major + ":" + minor + " rwm";
        try {
          cGroupsHandler.updateCGroupParam(CGroupsHandler.CGroupController.DEVICES,
              containerIdStr, CGroupsHandler.CGROUP_PARAM_DEVICES_ALLOW, value);
        } catch (ResourceHandlerException e) {
          cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
              containerIdStr);
          LOG.error("Cannot set {} with value {} to container {}", CGroupsHandler.CGROUP_PARAM_DEVICES_ALLOW,
              value, containerIdStr);
          throw e;
        }
      }
    }

    return null;
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

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId) throws ResourceHandlerException {
    deviceMappingManager.recoverAssignedDevices(resourceName, containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container) throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId) throws ResourceHandlerException {
    deviceMappingManager.cleanupAssignedDevices(resourceName, containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }
}
