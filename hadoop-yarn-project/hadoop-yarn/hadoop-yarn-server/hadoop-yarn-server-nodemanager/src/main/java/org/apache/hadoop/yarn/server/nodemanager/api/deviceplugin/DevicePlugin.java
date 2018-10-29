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

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

import java.util.Set;

/**
 * A must interface for vendor plugin to implement.
 * */
public interface DevicePlugin {
  /**
   * Called first when device plugin framework wants to register
   * @return DeviceRegisterRequest {@link DeviceRegisterRequest}
   * */
  DeviceRegisterRequest getRegisterRequestInfo();

  /**
   * Called when update node resource
   * @return a set of {@link Device}, {@link java.util.TreeSet} recommended
   * */
  Set<Device> getDevices();

  /**
   * Asking how these devices should be prepared/used before/when container launch.
   * @param allocatedDevices A set of allocated {@link Device}.
   * @param runtime Indicate which runtime the framework will use
   *        Could be {@code RUNTIME_CGROUPS} or {@code RUNTIME_DOCKER}
   *        in {@link DeviceRuntimeSpec}
   * @return a {@link DeviceRuntimeSpec} description about environment,
   * {@link VolumeSpec}, {@link MountVolumeSpec}. etc
   * */
  DeviceRuntimeSpec onDeviceUse(Set<Device> allocatedDevices, String runtime);

  /**
   * Called after device released.
   * */
  void onDevicesReleased(Set<Device> releasedDevices);
}
