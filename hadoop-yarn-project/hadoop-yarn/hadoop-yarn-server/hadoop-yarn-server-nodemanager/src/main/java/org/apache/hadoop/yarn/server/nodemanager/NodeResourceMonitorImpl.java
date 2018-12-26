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

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.DeviceMappingManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.DevicePluginAdapter;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Implementation of the node resource monitor. It periodically tracks the
 * resource utilization of the node and reports it to the NM.
 */
public class NodeResourceMonitorImpl extends AbstractService implements
    NodeResourceMonitor {

  /** Logging infrastructure. */
  final static Logger LOG =
       LoggerFactory.getLogger(NodeResourceMonitorImpl.class);

  /** Interval to monitor the node resource utilization. */
  private long monitoringInterval;
  /** Thread to monitor the node resource utilization. */
  private MonitoringThread monitoringThread;

  /** Resource calculator. */
  private ResourceCalculatorPlugin resourceCalculatorPlugin;

  /** Current <em>resource utilization</em> of the node. */
  private ResourceUtilization nodeUtilization;

  /**
   * If monitor pluggable device is needed.
   * */
  private boolean pluggableDeviceMonitorEnabled;

  /**
   * Interval pluggable device interval.
   * -1.0f means disable monitor
   * */
  private double pluggableDeviceMonitorInterval;

  /**
   * Last timestamp that we monitor devices
   * */
  private long lastDeviceMonitorTimestamp;


  private Context nmContext;

  /**
   * Initialize the node resource monitor.
   */
  public NodeResourceMonitorImpl(Context context) {
    super(NodeResourceMonitorImpl.class.getName());
    this.nmContext = context;
    this.monitoringThread = new MonitoringThread();
  }

  /**
   * Initialize the service with the proper parameters.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.monitoringInterval =
        conf.getLong(YarnConfiguration.NM_RESOURCE_MON_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_RESOURCE_MON_INTERVAL_MS);

    boolean deviceFrameworkEnabled =
        conf.getBoolean(
            YarnConfiguration.NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED,
            YarnConfiguration.DEFAULT_NM_PLUGGABLE_DEVICE_FRAMEWORK_ENABLED);

    this.resourceCalculatorPlugin =
        ResourceCalculatorPlugin.getNodeResourceMonitorPlugin(conf);

    LOG.info(" Using ResourceCalculatorPlugin : "
        + this.resourceCalculatorPlugin);

    // Check if the framework is enabled
    if (deviceFrameworkEnabled) {
      pluggableDeviceMonitorInterval = conf.getDouble(
          YarnConfiguration.NM_PLUGGABLE_DEVICE_MONITOR_INTERVAL,
          YarnConfiguration.DEFAULT_NM_PLUGGABLE_DEVICE_MONITOR_INTERVAL);
      // negative means disabled
      if (this.pluggableDeviceMonitorInterval < 0) {
        LOG.info("Monitor pluggable device is disabled. Interval-Hour:"
            + this.pluggableDeviceMonitorInterval);
        pluggableDeviceMonitorEnabled = false;
      }
      pluggableDeviceMonitorEnabled = true;
      if (!isEnabled() && pluggableDeviceMonitorEnabled) {
        LOG.warn("Please enable node monitoring first by a correct setting of "
            + YarnConfiguration.NM_RESOURCE_MON_INTERVAL_MS);
        LOG.warn("Monitor pluggable device is disabled");
        pluggableDeviceMonitorEnabled = false;
      }
      if (pluggableDeviceMonitorEnabled) {
        LOG.info("Monitor pluggable device is enabled. Interval-Hour: "
            + pluggableDeviceMonitorInterval);
      }
    }
  }

  /**
   * Check if we should be monitoring.
   * @return <em>true</em> if we can monitor the node resource utilization.
   */
  private boolean isEnabled() {
    if (this.monitoringInterval <= 0) {
      LOG.info("Node Resource monitoring interval is <=0. "
          + this.getClass().getName() + " is disabled.");
      return false;
    }
    if (resourceCalculatorPlugin == null) {
      LOG.info("ResourceCalculatorPlugin is unavailable on this system. "
          + this.getClass().getName() + " is disabled.");
      return false;
    }
    return true;
  }

  /**
   * Start the thread that does the node resource utilization monitoring.
   */
  @Override
  protected void serviceStart() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.start();
    }
    super.serviceStart();
  }

  /**
   * Stop the thread that does the node resource utilization monitoring.
   */
  @Override
  protected void serviceStop() throws Exception {
    if (this.isEnabled()) {
      this.monitoringThread.interrupt();
      try {
        this.monitoringThread.join(10 * 1000);
      } catch (InterruptedException e) {
        LOG.warn("Could not wait for the thread to join");
      }
    }
    super.serviceStop();
  }

  /**
   * Thread that monitors the resource utilization of this node.
   */
  private class MonitoringThread extends Thread {
    /**
     * Initialize the node resource monitoring thread.
     */
    public MonitoringThread() {
      super("Node Resource Monitor");
      this.setDaemon(true);
    }

    /**
     * Periodically monitor the resource utilization of the node.
     */
    @Override
    public void run() {
      while (true) {
        // Get node utilization and save it into the health status
        long pmem = resourceCalculatorPlugin.getPhysicalMemorySize() -
            resourceCalculatorPlugin.getAvailablePhysicalMemorySize();
        long vmem =
            resourceCalculatorPlugin.getVirtualMemorySize()
                - resourceCalculatorPlugin.getAvailableVirtualMemorySize();
        float vcores = resourceCalculatorPlugin.getNumVCoresUsed();
        nodeUtilization =
            ResourceUtilization.newInstance(
                (int) (pmem >> 20), // B -> MB
                (int) (vmem >> 20), // B -> MB
                vcores); // Used Virtual Cores
        if (pluggableDeviceMonitorEnabled) {
          mayUpdateNodeDevicesUtilization(nodeUtilization);
        }
        // Publish the node utilization metrics to node manager
        // metrics system.
        NodeManagerMetrics nmMetrics = nmContext.getNodeManagerMetrics();
        if (nmMetrics != null) {
          nmMetrics.setNodeUsedMemGB(nodeUtilization.getPhysicalMemory());
          nmMetrics.setNodeUsedVMemGB(nodeUtilization.getVirtualMemory());
          nmMetrics.setNodeCpuUtilization(nodeUtilization.getCPU());
        }

        try {
          Thread.sleep(monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn(NodeResourceMonitorImpl.class.getName()
              + " is interrupted. Exiting.");
          break;
        }
      }
    }

    private void mayUpdateNodeDevicesUtilization(ResourceUtilization nodeUti) {
      long current = System.currentTimeMillis();
      if ((current - lastDeviceMonitorTimestamp) <
          pluggableDeviceMonitorInterval * 3600000) {
        LOG.info("Not the perfect time to get device info");
        return;
      }
      LOG.info("Start querying the latest devices info");
      lastDeviceMonitorTimestamp = System.currentTimeMillis();
      Map<String, ResourcePlugin> plugins =
          nmContext.getResourcePluginManager().getNameToPlugins();
      DeviceMappingManager dpm =
          nmContext.getResourcePluginManager().getDeviceMappingManager();
      Map<String, Map<Device, ContainerId>> allUsed = dpm.getAllUsedDevices();
      for (Map.Entry<String, ResourcePlugin> entry : plugins.entrySet()) {
        if (entry.getValue() instanceof DevicePluginAdapter) {
          DevicePluginAdapter dpa = (DevicePluginAdapter)entry.getValue();
          DevicePlugin dp = dpa.getDevicePlugin();
          try {
            Set<Device> devices = dp.getDevices();
            try {
              nodeUti.setResourceValue(entry.getKey(), devices.size());
              nodeUti.setUsedResourceValue(entry.getKey(),
                  allUsed.get(entry.getKey()).size());
              // update deviceMappingManager's state
              dpm.updateDeviceSet(entry.getKey(), devices);
            } catch (Exception e) {
              LOG.warn("Unexpected exception in updating node deivces:");
              e.printStackTrace();
            }
          } catch (Exception e) {
            LOG.warn("Unexpected exception in {}'s method getDevices. {}",
                dp.getClass(), e.getMessage());
          }
        }
      } // end for
    }
  }

  /**
   * Get the <em>resource utilization</em> of the node.
   * @return <em>resource utilization</em> of the node.
   */
  @Override
  public ResourceUtilization getUtilization() {
    return this.nodeUtilization;
  }
}
