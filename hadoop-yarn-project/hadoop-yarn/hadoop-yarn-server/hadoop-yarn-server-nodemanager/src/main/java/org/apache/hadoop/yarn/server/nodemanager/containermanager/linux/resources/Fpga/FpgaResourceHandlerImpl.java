/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.Fpga;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@InterfaceStability.Unstable
@InterfaceAudience.Private
public class FpgaResourceHandlerImpl implements FpgaResourceHandler {

  static final Log LOG = LogFactory.getLog(FpgaResourceHandlerImpl.class);

  private final String REQUEST_FPGA_IP_ID_KEY = "REQUESTED_FPGA_IP_ID";

  private FpgaPluginChain pluginChain;

  private FpgaResourceAllocator allocator;

  private CGroupsHandler cGroupsHandler;

  public FpgaResourceHandlerImpl(CGroupsHandler cGroupsHandler, Configuration conf) {

    LOG.info("FPGA Plugin Chain init.");

    allocator = new FpgaResourceAllocator();
    //init all plugins based on configurations or hardcode
    pluginChain = new FpgaPluginChain();

    String[] fpgaPluginClassStrs = conf.getStrings(YarnConfiguration.NM_FPGA_PLUGIN_CLASS);
    if(fpgaPluginClassStrs == null) {
      LOG.info("No FPGA plugin can be loaded.");
    } else {

      for (String fpgaPluginClass : fpgaPluginClassStrs) {
        LOG.info("FPGA Plugin Class " + fpgaPluginClass);
        try {
          Constructor<?> constructor = Class.forName(fpgaPluginClass).getConstructor();
          AbstractFpgaPlugin fpgaPlugin = (AbstractFpgaPlugin) constructor.newInstance();
          pluginChain.addPlugin(fpgaPlugin);
          LOG.info(fpgaPluginClass + " loaded");
        } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
          throw new YarnRuntimeException(e);
        }
      }
    }
    this.cGroupsHandler = cGroupsHandler;
  }

  @VisibleForTesting
  public void addFpgaPlugin(AbstractFpgaPlugin plugin) {
    pluginChain.addPlugin(plugin);
  }

  @VisibleForTesting
  public static String getDeviceDeniedValue(int deviceMajorNumber, int deviceMinorNumber) {
    String val = String.format("c %d:%d rwm", deviceMajorNumber, deviceMinorNumber);
    LOG.info("Add denied devices to cgroups:" + val);
    return val;
  }

  @VisibleForTesting
  public FpgaResourceAllocator getFpgaAllocator() {
    return allocator;
  }

  public String getRequestedIPID(Container container) {
    Map<String, String> envs = container.getLaunchContext().getEnvironment();
    return envs.get(REQUEST_FPGA_IP_ID_KEY);
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration) throws ResourceHandlerException {
    // get vendor plugin type, major and minor number from configuration
    // add FPGA devices to allocator
    if (!pluginChain.initPlugin()){
      throw new ResourceHandlerException("Fpga plugin initialization failed", null);
    }
    //get major number and minor number from configuration node-resource.xml
    Map<String, String> allowed = ResourceUtils.getResourceTypeAllowedValue(configuration);
    for (AbstractFpgaPlugin plugin : pluginChain.getPlugins()) {
      if (allowed.keySet().contains(plugin.getFpgaType())) {
        String temp = allowed.get(plugin.getFpgaType());
        String[] parts = temp.split(",");
        for (String deviceNumber : parts) {
          String[] majorAndMinor = deviceNumber.split(":");
          if (majorAndMinor.length == 2) {
            int major = Integer.parseInt(majorAndMinor[0]);
            int minor = Integer.parseInt(majorAndMinor[1]);
            allocator.addFpga(plugin.getFpgaType(), major, minor,
                plugin.getExistingIPID(major, minor));
          } else {
            LOG.warn("wrong format of allowed device value:" + temp);
          }
        }
      } else {
        LOG.warn("no allowed device number configured for FPGA plugin:" + plugin.getFpgaType());
      }
    }
    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container) throws ResourceHandlerException {
    //1. get requested FPGA type and count, choose corresponding FPGA vendor plugin
    //2. use allocator.assignFpga(type, count) to get FPGAAllocation
    //3. downloadIP and configureIP
    //4. isolate device
    List<PrivilegedOperation> ret = new ArrayList<>();
    String containerIdStr = container.getContainerId().toString();
    Map<String, ResourceInformation> requestedResources = container.getResource().getResources();

    // Create device cgroups for the container
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerIdStr);

    AbstractFpgaPlugin tempPlugin;
    long deviceCount;
    String resourceName;
    String ipFilePath;
    for (Map.Entry<String, ResourceInformation> entry : requestedResources.entrySet()) {
      resourceName = entry.getKey();
      //find the proper plugin to handle the matching resource name
      tempPlugin = pluginChain.getPlugin(resourceName);
      if (tempPlugin != null) {
        //we have plugin that can handle this FPGA device request, allocate real device
        deviceCount = entry.getValue().getValue();
        FpgaResourceAllocator.FpgaAllocation allocation = allocator.assignFpga(
            resourceName, deviceCount, containerIdStr, getRequestedIPID(container));
        LOG.info("FpgaAllocation:" + allocation);
        if (null == allocation) {
          LOG.warn("null allocation for FPGA type: " + resourceName + ", requestor:" + containerIdStr);
          throw new ResourceHandlerException("Not enough devices! request:" + deviceCount + ",available:" + allocator.getAvailableFpgaCount());
        }
        try {
          //update cgroup device param
          for (FpgaResourceAllocator.FpgaDevice device : allocation.getDenied()) {
            cGroupsHandler.updateCGroupParam(
                CGroupsHandler.CGroupController.DEVICES, containerIdStr,
                CGroupsHandler.CGROUP_PARAM_DEVICE_DENY,
                getDeviceDeniedValue(device.getMajor(), device.getMinor()));
          }
          //downloadIp and configure IP
          ipFilePath = tempPlugin.downloadIP(getRequestedIPID(container), container.getWorkDir());
          if (null == ipFilePath) {
            throw new ResourceHandlerException("Fpga plugin failed to download IP", null);
          }
          List<FpgaResourceAllocator.FpgaDevice> allowed = allocation.getAllowed();
          List<String> addresses = new ArrayList<>();
          for(int i = 0; i < allowed.size(); i++) {
            addresses.add(allowed.get(i).getMajor() + ":" + allowed.get(i).getMinor());
          }

          if (!tempPlugin.configureIP(ipFilePath, addresses)) {
            throw new ResourceHandlerException("Fpga plugin failed to configure IP", null);
          }
          //update the allocator that we update an IP of a device
          allocator.updateFpga(containerIdStr, allocation, getRequestedIPID(container));
          //TODO: update the node constraint label
        } catch (ResourceHandlerException re) {
          cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
              containerIdStr);
          throw re;
        }
        //isolation operation
        ret.add(new PrivilegedOperation(
            PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
            PrivilegedOperation.CGROUP_ARG_PREFIX
                + cGroupsHandler.getPathForCGroupTasks(
                CGroupsHandler.CGroupController.DEVICES, containerIdStr)));
      }//end if
    }//end for
    return ret;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId) throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId) throws ResourceHandlerException {
    allocator.cleanupAssignFpgas(containerId.toString());
    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
        containerId.toString());
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }
}
