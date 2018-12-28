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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.examples;

import org.apache.hadoop.yarn.server.nodemanager.NodeResourceMonitorImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

public class SampleDevicePlugin implements DevicePlugin {
  private final static Logger LOG =
      LoggerFactory.getLogger(SampleDevicePlugin.class);

  @Override
  public DeviceRegisterRequest getRegisterRequestInfo() throws Exception {
    return DeviceRegisterRequest.Builder.newInstance()
        .setResourceName("sample.com/sample").build();
  }

  @Override
  public Set<Device> getDevices() throws Exception {
    Random random = new Random();
    TreeSet<Device> r = new TreeSet<>();
    int count = random.nextInt(10) + 1;
    LOG.info("Random device count: " + count);
    for (int i = 0; i < count; i++) {
      r.add(Device.Builder.newInstance()
          .setId(i)
          .setDevPath("/dev/sample" + i)
          .setMajorNumber(123)
          .setMinorNumber(i)
          .setBusID("0000:22:00." + i)
          .setHealthy(true)
          .build());
    }
    return r;
  }

  @Override
  public DeviceRuntimeSpec onDevicesAllocated(Set<Device> allocatedDevices,
      YarnRuntimeType yarnRuntime) throws Exception {
    return null;
  }

  @Override
  public void onDevicesReleased(Set<Device> releasedDevices) throws Exception {

  }
}
