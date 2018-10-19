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

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

import java.io.Serializable;
import java.util.Objects;

public class VolumeSpec implements Serializable, Comparable {

  private static final long serialVersionUID = 1L;

  private final String volumeDriver;
  private final String volumeName;
  private final String volumeOperation;

  public final static String CREATE = "create";
  public final static String DELETE = "delete";

  private VolumeSpec(Builder builder) {
    this.volumeDriver = builder.volumeDriver;
    this.volumeName = builder.volumeName;
    this.volumeOperation = builder.volumeOperation;
  }

  public String getVolumeDriver() {
    return volumeDriver;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getVolumeOperation() {
    return volumeOperation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()){
      return false;
    }
    VolumeSpec other = (VolumeSpec) o;
    return Objects.equals(volumeDriver, other.volumeDriver) &&
        Objects.equals(volumeName, other.volumeName) &&
        Objects.equals(volumeOperation, other.volumeOperation);
  }

  @Override
  public int compareTo(Object o) {
    return 0;
  }

  public static class Builder {
    private String volumeDriver;
    private String volumeName;
    private String volumeOperation;

    private Builder(){}

    public static Builder newInstance () {
      return new Builder();
    }

    public VolumeSpec build() {
      return new VolumeSpec(this);
    }

    public Builder setVolumeDriver(String volumeDriver) {
      this.volumeDriver = volumeDriver;
      return this;
    }

    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public Builder setVolumeOperation(String volumeOperation) {
      this.volumeOperation = volumeOperation;
      return this;
    }

  }
}
