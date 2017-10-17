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


package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaDiscoverer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.IntelFPGAOpenclPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.FPGA_URI;

@InterfaceStability.Unstable
@InterfaceAudience.Private
public class FpgaResourceHandlerImpl implements ResourceHandler {

  static final Log LOG = LogFactory.getLog(FpgaResourceHandlerImpl.class);

  private final String REQUEST_FPGA_IP_ID_KEY = "REQUESTED_FPGA_IP_ID";

  private IntelFPGAOpenclPlugin openclPlugin;

  private FpgaResourceAllocator allocator;

  private CGroupsHandler cGroupsHandler;

  public static final String EXCLUDED_FPGAS_CLI_OPTION = "--excluded_gpus";
  public static final String CONTAINER_ID_CLI_OPTION = "--container_id";
  private PrivilegedOperationExecutor privilegedOperationExecutor;

  public FpgaResourceHandlerImpl(Context nmContext,
      CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    allocator = new FpgaResourceAllocator(nmContext);
    // we only support Intel FPGA for OpenCL plugin at present.
    // define more general interfaces to support Xilinx or AWS EC2 FPGA SDK etc.
    openclPlugin = new IntelFPGAOpenclPlugin();
    FpgaDiscoverer.getInstance().setResourceHanderPlugin(openclPlugin);
    this.cGroupsHandler = cGroupsHandler;
    this.privilegedOperationExecutor = privilegedOperationExecutor;
  }

  @VisibleForTesting
  public FpgaResourceHandlerImpl(Context nmContext,
      CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor,
      IntelFPGAOpenclPlugin plugin) {
    allocator = new FpgaResourceAllocator(nmContext);
    // we only support Intel FPGA for OpenCL plugin at present.
    // define more general interfaces to support Xilinx or AWS EC2 FPGA SDK etc.
    openclPlugin = plugin;
    FpgaDiscoverer.getInstance().setResourceHanderPlugin(openclPlugin);
    this.cGroupsHandler = cGroupsHandler;
    this.privilegedOperationExecutor = privilegedOperationExecutor;
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
    return container.getLaunchContext().getEnvironment().
        get(REQUEST_FPGA_IP_ID_KEY);
  }

  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration) throws ResourceHandlerException {
    if (!openclPlugin.initPlugin()) {
      throw new ResourceHandlerException("FPGA plugin initialization failed", null);
    }
    LOG.info("FPGA Plugin bootstrap success.");
    // get avialable devices minor numbers from toolchain or static configuration
    List<FpgaResourceAllocator.FpgaDevice> fpgaDeviceList = FpgaDiscoverer.getInstance().discover();
    allocator.addFpga(openclPlugin.getFpgaType(), fpgaDeviceList);
    this.cGroupsHandler.initializeCGroupController(CGroupsHandler.CGroupController.DEVICES);
    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container) throws ResourceHandlerException {
    // 1. get requested FPGA type and count, choose corresponding FPGA plugin(s)
    // 2. download to ensure IP file exists
    // 2. use allocator.assignFpga(type, count) to get FPGAAllocation
    // 3. configure IP file. if failed, it's ok for opencl application
    // 4. isolate device
    List<PrivilegedOperation> ret = new ArrayList<>();
    String containerIdStr = container.getContainerId().toString();
    Resource requestedResource = container.getResource();

    // Create device cgroups for the container
    cGroupsHandler.createCGroup(CGroupsHandler.CGroupController.DEVICES,
      containerIdStr);

    long deviceCount = requestedResource.getResourceValue(FPGA_URI);
    LOG.info(containerIdStr + " requested " + deviceCount + " Intel FPGA(s)");
    String ipFilePath;
    try {
      //download IP
      ipFilePath = openclPlugin.downloadIP(getRequestedIPID(container), container.getWorkDir());
      if ("".equals(ipFilePath)) {
        throw new ResourceHandlerException("FPGA plugin failed to download IP", null);
      }
      //allocate
      FpgaResourceAllocator.FpgaAllocation allocation = allocator.assignFpga(
          openclPlugin.getFpgaType(), deviceCount,
          container, getRequestedIPID(container));
      LOG.info("FpgaAllocation:" + allocation);

      PrivilegedOperation privilegedOperation = new PrivilegedOperation(PrivilegedOperation.OperationType.FPGA,
          Arrays.asList(CONTAINER_ID_CLI_OPTION, containerIdStr));
      if (!allocation.getDenied().isEmpty()) {
        List<Integer> denied = new ArrayList<>();
        allocation.getDenied().forEach(device -> denied.add(device.getMinor()));
        privilegedOperation.appendArgs(Arrays.asList(EXCLUDED_FPGAS_CLI_OPTION,
            StringUtils.join(",", denied)));
      }
      privilegedOperationExecutor.executePrivilegedOperation(privilegedOperation, true);

      // configure IP
      LOG.info("IP file patch:" + ipFilePath);
      List<FpgaResourceAllocator.FpgaDevice> allowed = allocation.getAllowed();
      String majorMinorNumber;
      for (int i = 0; i < allowed.size(); i++) {
        majorMinorNumber = allowed.get(i).getMajor() + ":" + allowed.get(i).getMinor();
        String currentIPID = allowed.get(i).getIPID();
        if (null != currentIPID &&
            currentIPID.equalsIgnoreCase(getRequestedIPID(container))) {
          LOG.info("IP already in device \"" + allowed.get(i).getAliasDevName() + "," +
              majorMinorNumber + "\", skip reprogramming");
          continue;
        }
        if (openclPlugin.configureIP(ipFilePath, majorMinorNumber)) {
          // update the allocator that we update an IP of a device
          allocator.updateFpga(containerIdStr, allowed.get(i),
              getRequestedIPID(container));
        }
      }
      //TODO: update the node constraint label
    } catch (ResourceHandlerException re) {
      cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,
          containerIdStr);
      throw re;
    } catch (PrivilegedOperationException e) {
      cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES, containerIdStr);
      LOG.warn("Could not update cgroup for container", e);
    }
    //isolation operation
    ret.add(new PrivilegedOperation(
        PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
        PrivilegedOperation.CGROUP_ARG_PREFIX
        + cGroupsHandler.getPathForCGroupTasks(
        CGroupsHandler.CGroupController.DEVICES, containerIdStr)));
    return ret;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId) throws ResourceHandlerException {
    allocator.recoverAssignedFpgas(containerId);
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
