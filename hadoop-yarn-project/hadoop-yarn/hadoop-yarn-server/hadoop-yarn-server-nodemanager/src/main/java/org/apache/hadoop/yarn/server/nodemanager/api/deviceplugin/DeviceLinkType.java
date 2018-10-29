package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

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
