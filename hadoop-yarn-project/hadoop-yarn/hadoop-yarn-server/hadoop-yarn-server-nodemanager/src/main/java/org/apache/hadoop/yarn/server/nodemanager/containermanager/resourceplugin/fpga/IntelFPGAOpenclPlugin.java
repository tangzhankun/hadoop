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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga.FpgaResourceAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Intel FPGA for OpenCL plugin. Invoked by FpgaResourceHandlerImpl
 * The key points are:
 * 1. It uses Intel's toolchain "aocl" to reprogram IP to the device
 *    before container launch to achieve a quickest reprogramming path
 * 2. It avoids reprogramming by maintaining a mapping of device to FPGA IP ID
 */
public class IntelFPGAOpenclPlugin {
  public static final Logger LOG = LoggerFactory.getLogger(
      IntelFPGAOpenclPlugin.class);

  private InnerShellExecutor shell;

  protected static final String DEFAULT_BINARY_NAME = "aocl";

  protected static final String ALTERAOCLSDKROOT_NAME = "ALTERAOCLSDKROOT";

  private String pathToExecutable = null;

  // a mapping of major:minor number to acl0-31
  private Map<String, String> aliasMap;

  public String getDefaultBinaryName() {
    return DEFAULT_BINARY_NAME;
  }

  public String getDefaultPathToExecutable() {
    return System.getenv(ALTERAOCLSDKROOT_NAME);
  }

  public static String getDefaultPathEnvName() {
    return ALTERAOCLSDKROOT_NAME;
  }

  @VisibleForTesting
  public String getPathToExecutable() {
    return pathToExecutable;
  }

  public void setPathToExecutable(String pathToExecutable) {
    this.pathToExecutable = pathToExecutable;
  }

  @VisibleForTesting
  public void setShell(InnerShellExecutor shell) {
    this.shell = shell;
  }

  public Map<String, String> getAliasMap() {
    return aliasMap;
  }

  /**
   * Check the Intel FPGA for OpenCL toolchain and
   * try to recover programmed device information
   * */
  public boolean initPlugin() {
    this.aliasMap = new HashMap<>();
    this.shell = new InnerShellExecutor();
    if (!FpgaDiscoverer.getInstance().diagnose()) {
      LOG.warn("Intel FPGA for OpenCL diagnose failed!");
      return false;
    }
    return true;
  }

  public List<FpgaResourceAllocator.FpgaDevice> discover(int timeout) {
    List<FpgaResourceAllocator.FpgaDevice> list = new LinkedList<>();
    String output;
    output = getDiagnoseInfo(timeout);
    if (null == output) {
      return list;
    }
    parseDiagnoseInfo(output, list);
    return list;
  }

  public static class InnerShellExecutor {

    // for instance, cat /sys/class/aclpci_nalla_pcie/aclnalla_pcie0/dev
    // return a string in format <major:minor>
    public String getMajorAndMinorNumber(String devName) {
      String output;
      String sysclass = "/sys/class/" +
          devName.replaceAll("\\d*$", "") + "/" + devName + "/dev";
      try {
        output = Shell.execCommand(new HashMap<>(),
            new String[]{"cat", sysclass}, 10 * 1000);
        if (output.contains(":")) {
          return output.trim();
        } else {
          LOG.warn("Unknown output of " + sysclass);
        }
      } catch (IOException e) {
        String msg =
            "Failed to get major-minor number from reading " + sysclass;
        LOG.warn(msg);
      }
      return null;
    }

    public String runDiagnose(String binary, int timeout) {
      String output = null;
      try {
        output = Shell.execCommand(new HashMap<>(),
            new String[]{binary, "diagnose"}, timeout);
      } catch (IOException e) {
        String msg =
            "Failed to execute " + DEFAULT_BINARY_NAME + ", exception message:" + e
                .getMessage() + ", continue ...";
        LOG.warn(msg);
      }
      return output;
    }

  }

  /**
   * One real sample output of Intel FPGA SDK 17.0's "aocl diagnose" is as below:
   * "
   * aocl diagnose: Running diagnose from /home/fpga/intelFPGA_pro/17.0/hld/board/nalla_pcie/linux64/libexec
   *
   * ------------------------- acl0 -------------------------
   * Vendor: Nallatech ltd
   *
   * Phys Dev Name  Status   Information
   *
   * aclnalla_pcie0Passed   nalla_pcie (aclnalla_pcie0)
   *                        PCIe dev_id = 2494, bus:slot.func = 02:00.00, Gen3 x8
   *                        FPGA temperature = 54.4 degrees C.
   *                        Total Card Power Usage = 31.7 Watts.
   *                        Device Power Usage = 0.0 Watts.
   *
   * DIAGNOSTIC_PASSED
   * ---------------------------------------------------------
   * "
   *
   * While per Intel's guide, the output(should be outdated or prior SDK version's) is as below:
   *
   * "
   * aocl diagnose: Running diagnostic from ALTERAOCLSDKROOT/board/<board_name>/
   * <platform>/libexec
   * Verified that the kernel mode driver is installed on the host machine.
   * Using board package from vendor: <board_vendor_name>
   * Querying information for all supported devices that are installed on the host
   * machine ...
   *
   * device_name Status Information
   *
   * acl0 Passed <descriptive_board_name>
   *             PCIe dev_id = <device_ID>, bus:slot.func = 02:00.00,
   *               at Gen 2 with 8 lanes.
   *             FPGA temperature=43.0 degrees C.
   * acl1 Passed <descriptive_board_name>
   *             PCIe dev_id = <device_ID>, bus:slot.func = 03:00.00,
   *               at Gen 2 with 8 lanes.
   *             FPGA temperature = 35.0 degrees C.
   *
   * Found 2 active device(s) installed on the host machine, to perform a full
   * diagnostic on a specific device, please run aocl diagnose <device_name>
   *
   * DIAGNOSTIC_PASSED
   * "
   * But this method only support the first output
   * */
  public void parseDiagnoseInfo(String output, List<FpgaResourceAllocator.FpgaDevice> list) {
    if (output.contains("DIAGNOSTIC_PASSED")) {
      Matcher headerStartMatcher = Pattern.compile("acl[0-31]").matcher(output);
      Matcher headerEndMatcher = Pattern.compile("(?i)DIAGNOSTIC_PASSED").matcher(output);
      int sectionStartIndex;
      int sectionEndIndex;
      String aliasName;
      while (headerStartMatcher.find()) {
        sectionStartIndex = headerStartMatcher.end();
        String section = null;
        aliasName = headerStartMatcher.group();
        while (headerEndMatcher.find(sectionStartIndex)) {
          sectionEndIndex = headerEndMatcher.start();
          section = output.substring(sectionStartIndex, sectionEndIndex);
          break;
        }
        if (null == section) {
          LOG.warn("Unsupported diagnose output");
          return;
        }
        // devName, \(.*\)
        // busNum, bus:slot.func\s=\s.*,
        // FPGA temperature\s=\s.*
        // Total\sCard\sPower\sUsage\s=\s.*
        String[] fieldRegexes = new String[]{"\\(.*\\)", "(?i)bus:slot.func\\s=\\s.*,",
            "(?i)FPGA temperature\\s=\\s.*", "(?i)Total\\sCard\\sPower\\sUsage\\s=\\s.*"};
        String[] fields = new String[4];
        String tempFieldValue;
        for (int i = 0; i < fieldRegexes.length; i++) {
          Matcher fieldMatcher = Pattern.compile(fieldRegexes[i]).matcher(section);
          if (!fieldMatcher.find()) {
            LOG.warn("Couldn't find " + fieldRegexes[i] + " pattern");
            continue;
          }
          tempFieldValue = fieldMatcher.group().trim();
          if (i == 0) {
            // special case for Device name
            fields[i] = tempFieldValue.substring(1, tempFieldValue.length() - 1);
          } else {
            String ss = tempFieldValue.split("=")[1].trim();
            fields[i] = ss.substring(0, ss.length() - 1);
          }
        }
        String majorMinorNumber = this.shell.getMajorAndMinorNumber(fields[0]);
        String[] mmn = majorMinorNumber.split(":");
        this.aliasMap.put(majorMinorNumber, aliasName);
        list.add(new FpgaResourceAllocator.FpgaDevice(getFpgaType(),
            Integer.parseInt(mmn[0]),
            Integer.parseInt(mmn[1]), null,
            fields[0], aliasName, fields[1], fields[2], fields[3]));
      }// end while
    }// end if
  }

  public String getDiagnoseInfo(int timeout) {
    return this.shell.runDiagnose(this.pathToExecutable,timeout);
  }

  public boolean diagnose(int timeout) {
    String output = getDiagnoseInfo(timeout);
    if (null != output && output.contains("DIAGNOSTIC_PASSED")) {
      return true;
    }
    return false;
  }

  /**
   * this is actually the opencl platform type
   * */
  public String getFpgaType() {
    return "IntelOpenCL";
  }

  public String downloadIP(String id, String dstDir) {
    // Assume .aocx IP file is distributed by DS to local dir
    // First check if <id>.aocx exists. Use it if it does.
    String r = "";
    File IPFilePath = new File(dstDir, id + ".aocx");
    if (IPFilePath.exists()) {
      r = dstDir + "/" + id + ".aocx";
    } else {
      //search local for file names containing the id
      File dir = new File(dstDir);
      File[] listOfFiles = dir.listFiles();
      if (null != listOfFiles) {
        for (File t : listOfFiles) {
          if (t.isFile() && t.getName().contains(id)) {
            r = dstDir + "/" + t.getName();
            break;
          }
        }
      }
    }
    return r;
  }

  /**
   * Program a list of devices.
   * It's ok for the offline "aocl program" failed because the application will always invoke API to program
   * The reason we do offline reprogramming is to make the application's program process faster
   * @param ipPath the absolute path to the aocx IP file
   * @param majorMinorNumber major:minor string
   * @return True or False
   * */
  public boolean configureIP(String ipPath, String majorMinorNumber) {
    // perform offline program the IP to get a quickest reprogramming sequence
    // we need a mapping of "major:minor" to "acl0" to issue command "aocl program <acl0> <ipPath>"
    Shell.ShellCommandExecutor shexec;
    String aclName;
    aclName = this.aliasMap.get(majorMinorNumber);
    shexec = new Shell.ShellCommandExecutor(
        new String[]{this.pathToExecutable, "program", aclName, ipPath});
    try {
      shexec.execute();
      if (0 == shexec.getExitCode()) {
        LOG.info("Intel aocl program " + ipPath + " to " + aclName + " successfully");
      } else {
        return false;
      }
    } catch (IOException e) {
      LOG.warn("Intel aocl program " + ipPath + " to " + aclName + " failed!");
      e.printStackTrace();
      return false;
    }
    return true;
  }
}
