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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import java.util.Objects;

/**
 * <p>
 * <code>TypedResourceUtilization</code> models the utilization of one
 * typed computer resources in the cluster. For instance, GPU/FPGA
 * </p>
 */
@Public
@Unstable
public abstract class TypedResourceUtilization
    implements Comparable<TypedResourceUtilization> {
  @Public
  @Unstable
  public static TypedResourceUtilization newInstance(
      Resource allowed, int used) {
    TypedResourceUtilization typedResourceUtilization =
        Records.newRecord(TypedResourceUtilization.class);
    typedResourceUtilization.setLastestCapability(allowed);
    typedResourceUtilization.setUsed(used);
    return typedResourceUtilization;
  }

  /**
   * Get the latest allowed resource.
   *
   * @return Resource object
   */
  @Public
  @Unstable
  public abstract Resource getLatestCapability();

  /**
   * Get used count of this resource.
   *
   * @return count
   */
  @Public
  @Unstable
  public abstract int getUsed();

  /**
   * Set the latest allowed resource.
   *
   * @param newCap latest capability
   */
  @Public
  @Unstable
  public abstract void setLastestCapability(Resource newCap);

  /**
   * Set used count of this resource.
   *
   * @param count count of used resource
   */
  @Public
  @Unstable
  public abstract void setUsed(int count);

  @Override
  public int hashCode() {
    return Objects.hash(getLatestCapability(), getUsed());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TypedResourceUtilization other = (TypedResourceUtilization) o;
    return getLatestCapability().equals(other.getLatestCapability())
        && getUsed() == other.getUsed();
  }

  @Override
  public int compareTo(TypedResourceUtilization o) {
    if (o == null || (!(o instanceof TypedResourceUtilization))) {
      return -1;
    }
    int result = getLatestCapability().compareTo(o.getLatestCapability());
    if (0 != result) {
      return result;
    }
    return getUsed() - o.getUsed();
  }
}
