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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.examples;

import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.*;

import java.util.Set;
import java.util.TreeSet;

public class FakeDevicePlugin
    implements DevicePlugin,DevicePluginScheduler {
  public final static String resourceName = "cmp.com/cmp";
  @Override
  public DeviceRegisterRequest register() {
    return DeviceRegisterRequest.Builder.newInstance()
        .setResourceName(resourceName)
        .setPluginVersion("v1.0").build();
  }

  @Override
  public Set<Device> getDevices() {
    TreeSet<Device> r = new TreeSet<>();
    r.add(Device.Builder.newInstance()
        .setID(0)
        .setDevPath("/dev/cmp0")
        .setMajorNumber(243)
        .setMinorNumber(0)
        .setBusID("0000:65:00.0")
        .setHealthy(true)
        .build());
    return r;
  }

  @Override
  public DeviceRuntimeSpec OnDevicesAllocated(Set<Device> allocatedDevices) {
    return null;
  }

  @Override
  public void OnDevicesReleased(Set<Device> allocatedDevices) {

  }

  @Override
  public Set<Device> allocateDevices(Set<Device> availableDevices, Integer count) {
    Set<Device> allocated = new TreeSet<>();
    int number = 0;
    for (Device d : availableDevices) {
      allocated.add(d);
      number++;
      if (number == count) {
        break;
      }
    }
    return allocated;
  }
}
