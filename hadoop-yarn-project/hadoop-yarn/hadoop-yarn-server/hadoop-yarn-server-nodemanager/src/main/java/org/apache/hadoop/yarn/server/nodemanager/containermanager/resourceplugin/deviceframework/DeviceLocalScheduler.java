package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Schedule device resource based on requirements
 * */
public class DeviceLocalScheduler {
  final static Log LOG = LogFactory.getLog(DeviceLocalScheduler.class);

  private Context nmContext;
  private static final int WAIT_MS_PER_LOOP = 1000;

  private Map<String, Set<Device>> allDevices = new TreeMap<>();
  public DeviceLocalScheduler(Context context) {
    nmContext = context;
  }

}
