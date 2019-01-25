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
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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

  private Map<DeviceLinkType, Set<Device>> typeToDevices = new HashMap<>();

  Map<Device, Set<DeviceLink>> deviceToNeighbors = new HashMap<>();

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
        }
      }
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
    // get topology
    try {
      String topo = shellExecutor.getTopologyInfo();
      parseTopo(topo);

    } catch (IOException e) {
      LOG.error("Error in getting GPU topology info. " +
          "Skip topology aware scheduling");
    }
    // Basic scheduling
    int number = 0;
    for (Device d : availableDevices) {
      allocation.add(d);
      number++;
      if (number == count) {
        break;
      }
    }
    return allocation;
  }

  /**
   * Sample topo output:
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
   *
   *
   * */
  public void parseTopo(String topo) {
    String[] lines = topo.split("\n");
    for (String line : lines) {
      line = line.trim();
      // To the end. No more metrics info
      if (line.startsWith("Legend")) {
        break;
      }
      // Skip header
      if (line.contains("Affinity")) {
        continue;
      }
      String[] tokens =
    }
  }

  /**
   * Represent a connection to another GPU device.
   * */
  public class DeviceLink implements Comparable {

    public int getMinor() {
      return minor;
    }

    public void setMinor(int min) {
      this.minor = min;
    }

    private int minor;
    DeviceLinkType linkType;

    public DeviceLinkType getLinkType() {
      return linkType;
    }

    public DeviceLink(int minorNumber, DeviceLinkType linType) {
      this.minor = minorNumber;
      this.linkType = linType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()){
        return false;
      }
      DeviceLink other = (DeviceLink) o;
      return Objects.equals(other.getMinor(), minor) &&
          Objects.equals(other.getLinkType(), linkType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(minor, linkType);
    }

    @Override
    public int compareTo(Object o) {
      if (o == null || (!(o instanceof DeviceLink))) {
        return -1;
      }
      DeviceLink other = (DeviceLink) o;

      return other.linkType.compareTo(getLinkType());
    }
  }

  /**
   * Different type of link
   * */
  public enum DeviceLinkType {
    /**
     * For Nvdia GPU NVLink
     * */
    P2PLinkNVLink(4),

    /**
     * Connected to same CPU (Same NUMA node)
     * */
    P2PLinkSameCPU(3),

    /**
     * Cross CPU through socket-level link (e.g. QPI).
     * Usually cross NUMA node
     * */
    P2PLinkCrossCPU(2),

    /**
     * Just need to traverse one PCIe switch to talk
     * */
    P2PLinkSingleSwitch(1),

    /**
     * Need to traverse multiple PCIe switch to talk
     * */
    P2PLinkMultiSwitch(0);

    // A higher link level means faster communication
    private int linkLevel;

    public int getLinkLevel() {
      return linkLevel;
    }

    DeviceLinkType(int linkLevel) {
      this.linkLevel = linkLevel;
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
}
