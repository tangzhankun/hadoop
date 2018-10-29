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
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;

import java.util.List;
import java.util.Set;

public class DeviceResourceHandlerImpl implements ResourceHandler {

  final static Log LOG = LogFactory.getLog(DeviceResourceHandlerImpl.class);

  private String resourceName;
  private DevicePlugin devicePlugin;
  private DeviceSchedulerManager deviceSchedulerManager;
  private CGroupsHandler cGroupsHandler;
  private PrivilegedOperationExecutor privilegedOperationExecutor;
  private DevicePluginAdapter devicePluginAdapter;

  public DeviceResourceHandlerImpl(String resourceName,
      DevicePlugin devicePlugin,
      DevicePluginAdapter devicePluginAdapter,
      DeviceSchedulerManager deviceSchedulerManager,
      CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperation) {
    this.devicePluginAdapter = devicePluginAdapter;
    this.resourceName = resourceName;
    this.devicePlugin = devicePlugin;
    this.cGroupsHandler = cGroupsHandler;
    this.privilegedOperationExecutor = privilegedOperation;
    this.deviceSchedulerManager = deviceSchedulerManager;
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
    deviceSchedulerManager.addDeviceSet(resourceName, availableDevices);
    // TODO: Init cgroups

    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container) throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    DeviceSchedulerManager.DeviceAllocation allocation = deviceSchedulerManager.assignDevices(
        resourceName, container);
    LOG.debug("Allocated to " +
        containerIdStr + ": " + allocation );

    DeviceRuntimeSpec deviceRuntimeSpec = devicePlugin.onDeviceUse(
        allocation.getAllowed(), DeviceRuntimeSpec.RUNTIME_CGROUPS);

    // cgroups operation based on allocation
    /**
     * TODO: implement a general container-executor device module to accept do isolation
     * */

    return null;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId) throws ResourceHandlerException {
    deviceSchedulerManager.recoverAssignedDevices(resourceName, containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container) throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId) throws ResourceHandlerException {
    deviceSchedulerManager.cleanupAssignedDevices(resourceName, containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }
}
