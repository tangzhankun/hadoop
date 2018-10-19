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

import java.util.*;

public class DeviceRuntimeSpec {
  /**
   * The runtime gives device framework a hint (not forced to) on which container
   * runtime can use this Spec.
   * If cgroups, these fields could be empty if no special requirement
   * since the framework already knows major and minor device number
   * in {@link Device}.
   * If docker, these fields below should be populated as needed
   */
  private final String runtime;
  private final Map<String, String> envs;
  private final Set<MountVolumeSpec> volumeMounts;
  private final Set<MountDeviceSpec> deviceMounts;
  private final Set<VolumeSpec> volumeClaims;

  public final static String RUNTIME_CGROUPS = "cgroups";
  public final static String RUNTIME_DOCKER = "docker";

  private DeviceRuntimeSpec(Builder builder) {
    this.runtime = builder.runtime;
    this.deviceMounts = builder.deviceMounts;
    this.envs = builder.envs;
    this.volumeClaims = builder.volumeClaims;
    this.volumeMounts = builder.volumeMounts;
  }

  public static class Builder {

    private String runtime;
    private Map<String, String> envs;
    private Set<MountVolumeSpec> volumeMounts;
    private Set<MountDeviceSpec> deviceMounts;
    private Set<VolumeSpec> volumeClaims;

    private Builder() {
      runtime = DeviceRuntimeSpec.RUNTIME_DOCKER;
      envs = new HashMap<>();
      volumeClaims = new TreeSet<>();
      deviceMounts = new TreeSet<>();
      volumeMounts = new TreeSet<>();
    }

    public static Builder newInstance() {
      return new Builder();
    }

    public DeviceRuntimeSpec build() {
      return new DeviceRuntimeSpec(this);
    }

    public Builder setRuntime(String runtime) {
      this.runtime = runtime;
      return this;
    }

    public Builder addVolumeSpec(VolumeSpec spec) {
      this.volumeClaims.add(spec);
      return this;
    }

    public Builder addMountVolumeSpec(MountVolumeSpec spec) {
      this.volumeMounts.add(spec);
      return this;
    }

    public Builder addMountDeviceSpec(MountDeviceSpec spec) {
      this.deviceMounts.add(spec);
      return this;
    }

    public Builder addEnv(String key, String value) {
      this.envs.put(Objects.requireNonNull(key),
          Objects.requireNonNull(value));
      return this;
    }

  }

}
