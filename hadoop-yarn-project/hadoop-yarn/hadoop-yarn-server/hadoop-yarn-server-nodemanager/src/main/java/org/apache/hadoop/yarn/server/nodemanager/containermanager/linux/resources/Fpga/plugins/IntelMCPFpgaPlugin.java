package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.Fpga.plugins;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.Fpga.AbstractFpgaPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.Fpga.FpgaResourceAllocator;

import java.util.List;

public class IntelMCPFpgaPlugin implements AbstractFpgaPlugin {

  static final Log LOG = LogFactory.getLog(IntelMCPFpgaPlugin.class);

  @Override
  public boolean initPlugin(String s, Configuration configuration) {
    LOG.info("Intel " + getFpgaType() + " FPGA initPlugin()");
    return true;
  }

  @Override
  public String getExistingIPID(int major, int minor) {
    return null;
  }

  @Override
  public String getFpgaType() {
    return "MCP";
  }

  @Override
  public String downloadIP(String id, String dstDir) {
    LOG.info("Intel " + getFpgaType() + " FPGA downloadIP() to " + dstDir);
    return "";
  }

  @Override
  public boolean configureIP(String ipPath, FpgaResourceAllocator.FpgaAllocation fpgaAllocations) {
    LOG.info("Intel " + getFpgaType() + " FPGA configureIP()");
    return true;
  }

  @Override
  public boolean cleanupFpgas(FpgaResourceAllocator.FpgaAllocation fpgaAllocations) {
    LOG.info("Intel " + getFpgaType() + " FPGA cleanupFpgas()");
    return true;
  }
}
