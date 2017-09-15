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



package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@InterfaceStability.Unstable
@InterfaceAudience.Private
public class FpgaResourceHandlerImpl implements FpgaResourceHandler {

    static final Log LOG = LogFactory.getLog(FpgaResourceHandlerImpl.class);

    private final String REQUEST_FPGA_IP_ID_KEY = "REQUESTED_FPGA_IP_ID";
    private final String REQUEST_FPGA_NUM = "REQUEST_FPGA_NUM";
    private FpgaPluginChain pluginChain;
    private AbstractFpgaPlugin openclPlugin;

    private FpgaResourceAllocator allocator;

    private CGroupsHandler cGroupsHandler;

    public static final String EXCLUDED_FPGAS_CLI_OPTION = "--excluded_gpus";
    public static final String CONTAINER_ID_CLI_OPTION = "--container_id";
    private PrivilegedOperationExecutor privilegedOperationExecutor;

    public FpgaResourceHandlerImpl(Context nmContext, CGroupsHandler cGroupsHandler,
                                   PrivilegedOperationExecutor privilegedOperationExecutor, Configuration conf) {
        LOG.info("FPGA Plugin Chain init.");
        allocator = new FpgaResourceAllocator(nmContext);
        pluginChain = new FpgaPluginChain();
        //we only support one opencl plugin now
        openclPlugin = new OpenclFpgaPlugin();
        addFpgaPlugin(openclPlugin);
        this.cGroupsHandler = cGroupsHandler;
        this.privilegedOperationExecutor = privilegedOperationExecutor;
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
        //get minor number from configuration yarn-site.xml
        String allowed = configuration.get(YarnConfiguration.NM_FPGA_ALLOWED_DEVICES);

        String[] miniors = allowed.split(",");
        for (String minorNumber : miniors) {
            int minor = Integer.parseInt(minorNumber);
            //majorNumber -1 is useless here
            allocator.addFpga("opencl", -1, minor,
                    openclPlugin.getExistingIPID(-1, minor));
        }
        this.cGroupsHandler.initializeCGroupController(CGroupsHandler.CGroupController.DEVICES);
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
        ResourceInformation[] requestedResources = container.getResource().getResources();

        // Create device cgroups for the container
        cGroupsHandler.createCGroup(CGroupsHandler.CGroupController.DEVICES,
                containerIdStr);

        AbstractFpgaPlugin tempPlugin;
        long deviceCount;
        String resourceName;
        String ipFilePath;
        for (ResourceInformation ri : requestedResources) {
            resourceName = ri.getName();
            deviceCount = ri.getValue();
            //find the proper plugin to handle the matching resource name
            tempPlugin = pluginChain.getPlugin(resourceName);
            if (tempPlugin != null && deviceCount != 0) {
                //we have plugin that can handle this FPGA device request, allocate real device
                deviceCount = ri.getValue();
                FpgaResourceAllocator.FpgaAllocation allocation = allocator.assignFpga(
                        resourceName, deviceCount, container.getContainerId(), getRequestedIPID(container));
                LOG.info("FpgaAllocation:" + allocation);
                if (null == allocation) {
                    LOG.warn("null allocation for FPGA type: " + resourceName + ", requestor:" + containerIdStr);
                    throw new ResourceHandlerException("Not enough devices! request:" + deviceCount + ",available:" + allocator.getAvailableFpgaCount());
                }
                try {
                    //update cgroup device param
                    for (FpgaResourceAllocator.FpgaDevice device : allocation.getDenied()) {
                        //we use GPU interface for now
                        PrivilegedOperation privilegedOperation = new PrivilegedOperation(PrivilegedOperation.OperationType.GPU, Arrays.asList(CONTAINER_ID_CLI_OPTION, containerIdStr));
                        if (!allocation.getDenied().isEmpty()) {
                            privilegedOperation.appendArgs(Arrays.asList(EXCLUDED_FPGAS_CLI_OPTION, StringUtils.join(",", allocation.getDenied())));
                        }
                        privilegedOperationExecutor.executePrivilegedOperation(privilegedOperation, true);
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
                } catch (PrivilegedOperationException e) {
                    cGroupsHandler.deleteCGroup(CGroupsHandler.CGroupController.DEVICES,containerIdStr);
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
