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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.nvidia.com;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Nvidia GPU plugin supporting both Nvidia Docker v2 and non-Docker container.
 * */
public class NvidiaGPUPlugin implements DevicePlugin {
  public static final Logger LOG = LoggerFactory.getLogger(
      NvidiaGPUPlugin.class);

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

  public NvidiaGPUPlugin() throws Exception {
    // search env for the binary
    String envBinaryPath = System.getenv(ENV_BINARY_PATH);
    if (null != envBinaryPath) {
      if (new File(envBinaryPath).exists()) {
        this.pathOfGpuBinary = envBinaryPath;
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
        this.pathOfGpuBinary = binaryFile.getAbsolutePath();
        LOG.info("Found script:" + this.pathOfGpuBinary);
        break;
      }
    }
    if (!found) {
      LOG.error("No binary found in below path"
          + DEFAULT_BINARY_SEARCH_DIRS.toString());
      throw new Exception("No binary found for " + NvidiaGPUPlugin.class);
    }
  }

  @Override
  public DeviceRegisterRequest getRegisterRequestInfo() throws Exception {
    return DeviceRegisterRequest.Builder.newInstance()
        .setResourceName("nvidia.com/gpu").build();
  }

  @Override
  public Set<Device> getDevices() throws Exception {
    TreeSet<Device> r = new TreeSet<>();
    String output;
    try {
      output = Shell.execCommand(environment,
          new String[]{pathOfGpuBinary, "--query-gpu=index,pci.bus_id",
              "--format=csv,noheader"}, MAX_EXEC_TIMEOUT_MS);

      String[] lines = output.split("\n");
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
    if (yarnRuntime == YarnRuntimeType.RUNTIME_DEFAULT) {
      return null;
    }
    if (yarnRuntime == YarnRuntimeType.RUNTIME_DOCKER) {
      String nvidiaRuntime = "nvidia";
      String nvidiaVisibleDevices = "NVIDIA_VISIBLE_DEVICES";
      String gpuMinorNumbers = "";
      for (Device device : allocatedDevices) {
        gpuMinorNumbers = gpuMinorNumbers + device.getMinorNumber() + ",";
      }
      LOG.info("Nvidia Docker v2 assigned GPU: " + gpuMinorNumbers);
      return DeviceRuntimeSpec.Builder.newInstance()
          .addEnv(nvidiaVisibleDevices,
              gpuMinorNumbers.substring(0, gpuMinorNumbers.length() - 1))
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
  public String getMajorNumber(String devName) {
    String output = null;
    // output "major:minor" in hex
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
        new String[]{"stat", "-c", "%t:%T", "/dev/" + devName});
    try {
      LOG.debug("Get major numbers from /dev/" + devName);
      shexec.execute();
      String[] strs = shexec.getOutput().trim().split(":");
      LOG.debug("stat output:" + shexec.getOutput());
      output = Integer.toString(Integer.parseInt(strs[0], 16));
    } catch (IOException e) {
      String msg =
          "Failed to get major number from reading /dev/" + devName;
      LOG.warn(msg);
      LOG.debug("Command output:" + shexec.getOutput() + ", exit code:" +
          shexec.getExitCode());
    }
    return output;
  }

  public class ShellExecutor {
    public String getDevNumber(String devName) {
      String output = null;
      // output "major:minor" in hex
      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
          new String[]{"stat", "-c", "%t:%T", "/dev/" + devName});
      try {
        LOG.debug("Get major numbers from /dev/" + devName);
        shexec.execute();
        output = shexec.getOutput().trim();
      } catch (IOException e) {
        String msg =
            "Failed to get major number from reading /dev/" + devName;
        LOG.warn(msg);
        LOG.debug("Command output:" + shexec.getOutput() + ", exit code:" +
            shexec.getExitCode());
      }
      return output;
    }
  }
}
