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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nvidia;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePluginScheduler;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Nvidia GPU plugin supporting both Nvidia Docker v2 and non-Docker container.
 * It has topology aware as well as simple scheduling ability.
 * */
public class NvidiaGPUPlugin implements DevicePlugin, DevicePluginScheduler {
  public static final Logger LOG = LoggerFactory.getLogger(
      NvidiaGPUPlugin.class);

  private MyShellExecutor shellExecutor = new MyShellExecutor();

  private Map<String, String> environment = new HashMap<>();

  // If this environment is set, use it directly
  private static final String ENV_BINARY_PATH = "NVIDIA_SMI_PATH";

  private static final String DEFAULT_BINARY_NAME = "nvidia-smi";

  private static final String DEV_NAME_PREFIX = "nvidia";

  private String pathOfGpuBinary = null;

  // command should not run more than 10 sec.
  private static final int MAX_EXEC_TIMEOUT_MS = 10 * 1000;

  // When executable path not set, try to search default dirs
  // By default search /usr/bin, /bin, and /usr/local/nvidia/bin (when
  // launched by nvidia-docker.
  private static final Set<String> DEFAULT_BINARY_SEARCH_DIRS = ImmutableSet.of(
      "/usr/bin", "/bin", "/usr/local/nvidia/bin");

  private boolean topoInitialized = false;

  private Set<Device> lastTimeFoundDevices;

  /**
   * It caches the combination of different devices and the communication cost.
   * The key is device count
   * The value is a map whose key is device combination, value is cost.
   * For instance:
   * { 2=> {[device1,device2]=>0, [device1,device3]=>10}
   *   3 => {[device1,device2,device3]=>10, [device2,device3,device5]=>20},
   * }
   * */
  private Map<Integer, Map<Set<Device>, Integer>> costTable = new HashMap<>();

  /**
   * The key is a pair of minors. For instance, "0-1" indicates 0 to 1
   * The value is weight between the two devices.
   * */
  private Map<String, Integer> devicePairToWeight = new HashMap<>();

  /**
   * The container set this environment variable to tell the scheduler what's
   * the policy to use when do scheduling
   * */
  public static final String TOPOLOGY_POLICY_ENV_KEY = "NVIDIA_TOPO_POLICY";

  /**
   * Schedule policy that prefer the faster GPU-GPU communication.
   * Suitable for heavy GPU computation workload generally.
   * */
  public static final String TOPOLOGY_POLICY_PACK = "PACK";

  /**
   * Schedule policy that prefer the faster CPU-GPU communication.
   * Suitable for heavy CPU-GPU IO operations generally.
   * */
  public static final String TOPOLOGY_POLICY_SPREAD = "SPREAD";

  @Override
  public DeviceRegisterRequest getRegisterRequestInfo() throws Exception {
    return DeviceRegisterRequest.Builder.newInstance()
        .setResourceName("nvidia.com/gpu").build();
  }

  @Override
  public Set<Device> getDevices() throws Exception {
    shellExecutor.searchBinary();
    TreeSet<Device> r = new TreeSet<>();
    String output;
    try {
      output = shellExecutor.getDeviceInfo();
      String[] lines = output.trim().split("\n");
      int id = 0;
      for (String oneLine : lines) {
        String[] tokensEachLine = oneLine.split(",");
        String minorNumber = tokensEachLine[0].trim();
        String busId = tokensEachLine[1].trim();
        String majorNumber = getMajorNumber(DEV_NAME_PREFIX
            + minorNumber);
        if (majorNumber != null) {
          r.add(Device.Builder.newInstance()
              .setId(id)
              .setMajorNumber(Integer.parseInt(majorNumber))
              .setMinorNumber(Integer.parseInt(minorNumber))
              .setBusID(busId)
              .setDevPath("/dev/" + DEV_NAME_PREFIX + minorNumber)
              .setHealthy(true)
              .build());
          id++;
        }
      }
      // cache it which help to topology scheduling
      lastTimeFoundDevices = r;
      return r;
    } catch (IOException e) {
      LOG.debug("Failed to get output from " + pathOfGpuBinary);
      throw new YarnException(e);
    }
  }

  @Override
  public DeviceRuntimeSpec onDevicesAllocated(Set<Device> allocatedDevices,
      YarnRuntimeType yarnRuntime) throws Exception {
    LOG.debug("Generating runtime spec for allocated devices: "
        + allocatedDevices + ", " + yarnRuntime.getName());
    if (yarnRuntime == YarnRuntimeType.RUNTIME_DEFAULT) {
      return null;
    }
    if (yarnRuntime == YarnRuntimeType.RUNTIME_DOCKER) {
      String nvidiaRuntime = "nvidia";
      String nvidiaVisibleDevices = "NVIDIA_VISIBLE_DEVICES";
      StringBuffer gpuMinorNumbersSB = new StringBuffer();
      for (Device device : allocatedDevices) {
        gpuMinorNumbersSB.append(device.getMinorNumber() + ",");
      }
      String minorNumbers = gpuMinorNumbersSB.toString();
      LOG.info("Nvidia Docker v2 assigned GPU: " + minorNumbers);
      return DeviceRuntimeSpec.Builder.newInstance()
          .addEnv(nvidiaVisibleDevices,
              minorNumbers.substring(0, minorNumbers.length() - 1))
          .setContainerRuntime(nvidiaRuntime)
          .build();
    }
    return null;
  }

  @Override
  public void onDevicesReleased(Set<Device> releasedDevices) throws Exception {
    // do nothing
  }

  // Get major number from device name.
  private String getMajorNumber(String devName) {
    String output = null;
    // output "major:minor" in hex
    try {
      LOG.debug("Get major numbers from /dev/" + devName);
      output = shellExecutor.getMajorMinorInfo(devName);
      String[] strs = output.trim().split(":");
      LOG.debug("stat output:" + output);
      output = Integer.toString(Integer.parseInt(strs[0], 16));
    } catch (IOException e) {
      String msg =
          "Failed to get major number from reading /dev/" + devName;
      LOG.warn(msg);
    }
    return output;
  }

  @Override
  public Set<Device> allocateDevices(Set<Device> availableDevices, int count,
      Map<String, String> envs) {
    Set<Device> allocation = new TreeSet<>();
    // corner cases
    if (count == 1 || count == availableDevices.size()) {
      basicSchedule(allocation, count, availableDevices);
      return allocation;
    }
    try {
      if (!topoInitialized) {
        initCostTable();
      }
      // topology aware scheduling
      topologyAwareSchedule(allocation, count,
          envs, availableDevices, this.costTable);
      if (allocation.size() != count) {
        LOG.error("Failed to do topology scheduling. Skip to use basic " +
            "scheduling");
      }
      return allocation;
    } catch (IOException e) {
      LOG.error("Error in getting GPU topology info. " +
          "Skip topology aware scheduling");
    }
    // basic scheduling
    basicSchedule(allocation, count, availableDevices);
    return allocation;
  }

  @VisibleForTesting
  public void initCostTable() throws IOException {
    // get topology
    String topo = shellExecutor.getTopologyInfo();
    // build the graph
    parseTopo(topo, devicePairToWeight);
    // build the cost table of different device combinations
    if (lastTimeFoundDevices == null) {
      try {
        getDevices();
      } catch (Exception e) {
        LOG.error("Failed to get devices!");
        return;
      }
    }
    buildCostTable(costTable, lastTimeFoundDevices);
    this.topoInitialized = true;
  }

  /**
   * Generate combination of devices and its cost.
   * costTable
   * */
  private void buildCostTable(
      Map<Integer, Map<Set<Device>, Integer>> costTable,
      Set<Device> lastTimeFoundDevices) {
    Device[] deviceList = new Device[lastTimeFoundDevices.size()];
    lastTimeFoundDevices.toArray(deviceList);
    generateAllDeviceCombination(costTable, deviceList, deviceList.length);
  }

  /**
   * For every possible combination of i elements.
   * We generate a map whose key is the combination, value is cost.
   */
  private void generateAllDeviceCombination(
      Map<Integer, Map<Set<Device>, Integer>> costTable,
      Device[] allDevices, int n) {
    // allocated devices count range from 1 to n-1
    for (int i = 2; i < n; i++) {
      Map<Set<Device>, Integer> combinationToCost =
          new HashMap<>();
      buildCombination(combinationToCost, allDevices, n, i);
      costTable.put(i, combinationToCost);
    }
  }

  private void buildCombination(Map<Set<Device>, Integer> combinationToCost,
      Device[] allDevices, int n, int r) {
    // A temporary list to store all combination one by one
    Device[] subDeviceList = new Device[r];
    combinationRecursive(combinationToCost, allDevices, subDeviceList,
        0, n - 1, 0, r);
  }

  /**
   * Populate combination to cost map recursively.
   *
   * @param cTc           combinationToCost map. The key is device set, the value is cost
   * @param allDevices    all devices used to assign value to subDevicelist
   * @param subDeviceList store a subset of devices temporary
   * @param start         start index in the allDevices
   * @param end           last index in the allDevices
   * @param index         dynamic index in the subDeviceList need to be assigned
   * @param r             the length of the subDeviceList
   */
  void combinationRecursive(Map<Set<Device>, Integer> cTc,
      Device[] allDevices, Device[] subDeviceList,
      int start, int end, int index, int r) {
    // sub device list's length is ready to compute the cost
    if (index == r) {
      Set<Device> oneSet = new TreeSet<>(Arrays.asList(subDeviceList));
      int cost = computeCostOfDevices(subDeviceList);
      cTc.put(oneSet, cost);
      return;
    }
    for (int i = start; i <= end; i++) {
      subDeviceList[index] = allDevices[i];
      combinationRecursive(cTc, allDevices, subDeviceList,
          i + 1, end, index + 1, r);
    }
  }

  /**
   * The cost function used to calculate costs of a sub set of devices.
   * It calculate link weight of each pair in non-duplicated combination of
   * devices.
   */
  private int computeCostOfDevices(Device[] devices) {
    int cost = 0;
    String gpuIndex0;
    String gpuIndex1;
    for (int i = 0; i < devices.length; i++) {
      gpuIndex0 = String.valueOf(devices[i].getMinorNumber());
      for (int j = i + 1; j < devices.length; j++) {
        gpuIndex1 = String.valueOf(devices[j].getMinorNumber());
        cost += this.devicePairToWeight.get(gpuIndex0 + "-" + gpuIndex1);
      }
    }
    return cost;
  }

  /**
   * Topology Aware schedule algorithm.
   * It doesn't consider CPU affinity or NUMA or bus bandwidths.
   * It support two plicy: "spread" and "pack" which can be set by container's
   * environment variable. Use pack by default which means prefer the faster
   * GPU-GPU. "Spread" means prefer the faster CPU-GPU.
   * It can potentially be extend to take GPU attribute like GPU chip memory
   * into consideration.
   * */
  @VisibleForTesting
  public void topologyAwareSchedule(Set<Device> allocation, int count,
      Map<String, String> envs,
      Set<Device> availableDevices,
      Map<Integer, Map<Set<Device>, Integer>> costTable) {
    int num = 0;
    String policy = envs.get(TOPOLOGY_POLICY_ENV_KEY);
    if (policy == null) {
      policy = TOPOLOGY_POLICY_PACK;
    }

    /**
     * Get combinations from costTable given the count of device want to
     * allocate.
     * */
    if (costTable == null) {
      LOG.error("No cost table initialized!");
      return;
    }
    Map<Set<Device>, Integer> combinationsToCost = costTable.get(count);
    List<Map.Entry<Set<Device>, Integer>> listSortedByCost =
        new LinkedList<>(combinationsToCost.entrySet());

    // the container needs PACK policy
    if (policy.equalsIgnoreCase(TOPOLOGY_POLICY_PACK)) {
      Collections.sort(listSortedByCost,
          (o1, o2) -> (o1.getValue()).compareTo(o2.getValue()));
      // search from low cost to high cost for combinations of count devices
      for (Map.Entry<Set<Device>, Integer> entry : listSortedByCost) {
        if (availableDevices.containsAll(entry.getKey())) {
          allocation.addAll(entry.getKey());
          LOG.info("Topology scheduler allocated: " + allocation);
          return;
        }
      }
      LOG.error("Unknown error happened in topology scheduler");
    }
    // the container needs spread policy
    if (policy.equalsIgnoreCase(TOPOLOGY_POLICY_SPREAD)) {
      Collections.sort(listSortedByCost,
          (o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));
      // search from high cost to low cost
      for (Map.Entry<Set<Device>, Integer> entry : listSortedByCost) {
        if (availableDevices.containsAll(entry.getKey())) {
          allocation.addAll(entry.getKey());
          LOG.info("Topology scheduler allocated: " + allocation);
          return;
        }
      }
      LOG.error("Unknown error happened in topology scheduler");
    }
  }

  @VisibleForTesting
  public void basicSchedule(Set<Device> allocation, int count,
      Set<Device> availableDevices) {
    // Basic scheduling
    // allocate all available
    if (count == availableDevices.size()) {
      allocation.addAll(availableDevices);
      return;
    }
    int number = 0;
    for (Device d : availableDevices) {
      allocation.add(d);
      number++;
      if (number == count) {
        break;
      }
    }
  }

  /**
   * A typical sample topo output:
   *
   *     GPU0	GPU1	GPU2	GPU3	CPU Affinity
   * GPU0	 X 	PHB	SOC	SOC	0-31
   * GPU1	PHB	 X 	SOC	SOC	0-31
   * GPU2	SOC	SOC	 X 	PHB	0-31
   * GPU3	SOC	SOC	PHB	 X 	0-31
   *
   *
   * Legend:
   *
   *   X   = Self
   *   SOC  = Connection traversing PCIe as well as the SMP link between
   *   CPU sockets(e.g. QPI)
   *   PHB  = Connection traversing PCIe as well as a PCIe Host Bridge
   *   (typically the CPU)
   *   PXB  = Connection traversing multiple PCIe switches
   *   (without traversing the PCIe Host Bridge)
   *   PIX  = Connection traversing a single PCIe switch
   *   NV#  = Connection traversing a bonded set of # NVLinks」
   * */
  public void parseTopo(String topo,
      Map<String, Integer> deviceLinkToWeight) {
    String[] lines = topo.split("\n");
    int rowMinor;
    int colMinor;
    String legend;
    String tempType;
    for (String oneLine : lines) {
      oneLine = oneLine.trim();
      if (oneLine.isEmpty()) {
        continue;
      }
      // To the end. No more metrics info
      if (oneLine.startsWith("Legend")) {
        break;
      }
      // Skip header
      if (oneLine.contains("Affinity")) {
        continue;
      }
      String[] tokens = oneLine.split(("\\s+"));
      String name = tokens[0];
      rowMinor = Integer.parseInt(name.substring(name.lastIndexOf("U") + 1));
      for (int i = 1; i < tokens.length; i++) {
        tempType = tokens[i];
        colMinor = i - 1;
        // self, skip
        if (tempType.equals("X")) {
          continue;
        }
        if (tempType.equals("SOC") || tempType.equals("SYS")) {
          populateGraphEdgeWeight(DeviceLinkType.P2PLinkCrossCPUSocket,
              rowMinor, colMinor, deviceLinkToWeight);
        }
        if (tempType.equals("PHB") || tempType.equals("NODE")) {
          populateGraphEdgeWeight(DeviceLinkType.P2PLinkSameCPUSocket,
              rowMinor, colMinor, deviceLinkToWeight);
        }
        if (tempType.equals("PXB")) {
          populateGraphEdgeWeight(DeviceLinkType.P2PLinkMultiSwitch,
              rowMinor, colMinor, deviceLinkToWeight);
        }
        if (tempType.equals("PIX")) {
          populateGraphEdgeWeight(DeviceLinkType.P2PLinkSingleSwitch,
              rowMinor, colMinor, deviceLinkToWeight);
        }
        if (tempType.startsWith("NV")) {
          populateGraphEdgeWeight(DeviceLinkType.P2PLinkNVLink,
              rowMinor, colMinor, deviceLinkToWeight);
        }
      } // end one line handling
    }
  }

  private void populateGraphEdgeWeight(
      DeviceLinkType linkType,
      int leftVertex,
      int rightVertex,
      Map<String, Integer> deviceLinkToWeight) {
    deviceLinkToWeight.putIfAbsent(leftVertex + "-" + rightVertex,
        linkType.getWeight());
  }

  /**
   * Different type of link
   * */
  public enum DeviceLinkType {
    /**
     * For Nvdia GPU NVLink
     * */
    P2PLinkNVLink(1),

    /**
     * Connected to same CPU (Same NUMA node)
     * */
    P2PLinkSameCPUSocket(2),

    /**
     * Cross CPU through socket-level link (e.g. QPI).
     * Usually cross NUMA node
     * */
    P2PLinkCrossCPUSocket(4),

    /**
     * Just need to traverse one PCIe switch to talk
     * */
    P2PLinkSingleSwitch(16),

    /**
     * Need to traverse multiple PCIe switch to talk
     * */
    P2PLinkMultiSwitch(32);

    // A higher link level means slower communication
    private int weight;

    public int getWeight() {
      return weight;
    }

    DeviceLinkType(int w) {
      this.weight = w;
    }
  }

  /**
   * A shell wrapper class easy for test.
   * */
  public class MyShellExecutor {

    public String getDeviceInfo() throws IOException {
      return Shell.execCommand(environment,
          new String[]{pathOfGpuBinary, "--query-gpu=index,pci.bus_id",
              "--format=csv,noheader"}, MAX_EXEC_TIMEOUT_MS);
    }

    public String getMajorMinorInfo(String devName) throws IOException {
      String output = null;
      // output "major:minor" in hex
      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
          new String[]{"stat", "-c", "%t:%T", "/dev/" + devName});
      shexec.execute();
      return shexec.getOutput();
    }

    // Get the topology metrics info from nvdia-smi
    public String getTopologyInfo() throws IOException {
      return Shell.execCommand(environment,
          new String[]{pathOfGpuBinary, "topo",
              "-m"}, MAX_EXEC_TIMEOUT_MS);
    }

    public void searchBinary() throws Exception {
      if (pathOfGpuBinary != null) {
        return;
      }
      // search env for the binary
      String envBinaryPath = System.getenv(ENV_BINARY_PATH);
      if (null != envBinaryPath) {
        if (new File(envBinaryPath).exists()) {
          pathOfGpuBinary = envBinaryPath;
          LOG.info("Use nvidia gpu binary: " + pathOfGpuBinary);
          return;
        }
      }
      LOG.info("Search script..");
      // search if binary exists in default folders
      File binaryFile;
      boolean found = false;
      for (String dir : DEFAULT_BINARY_SEARCH_DIRS) {
        binaryFile = new File(dir, DEFAULT_BINARY_NAME);
        if (binaryFile.exists()) {
          found = true;
          pathOfGpuBinary = binaryFile.getAbsolutePath();
          LOG.info("Found script:" + pathOfGpuBinary);
          break;
        }
      }
      if (!found) {
        LOG.error("No binary found in below path"
            + DEFAULT_BINARY_SEARCH_DIRS.toString());
        throw new Exception("No binary found for " + NvidiaGPUPlugin.class);
      }
    }
  }

  @VisibleForTesting
  public void setPathOfGpuBinary(String pathOfGpuBinary) {
    this.pathOfGpuBinary = pathOfGpuBinary;
  }

  @VisibleForTesting
  public void setShellExecutor(
      MyShellExecutor shellExecutor) {
    this.shellExecutor = shellExecutor;
  }

  @VisibleForTesting
  public boolean isTopoInitialized() {
    return topoInitialized;
  }

  @VisibleForTesting
  public Map<Integer, Map<Set<Device>, Integer>> getCostTable() {
    return costTable;
  }

  @VisibleForTesting
  public Map<String, Integer> getDevicePairToWeight() {
    return devicePairToWeight;
  }

}
