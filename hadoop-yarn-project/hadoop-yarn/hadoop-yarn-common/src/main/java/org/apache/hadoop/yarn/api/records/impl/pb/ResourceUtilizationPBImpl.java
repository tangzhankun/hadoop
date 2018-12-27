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

package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.TypedResourceUtilization;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceInformationProto;
import org.apache.hadoop.yarn.proto.YarnProtos.TypedResourceUtilizationProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceUtilizationProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceUtilizationProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

@Private
@Unstable
public class ResourceUtilizationPBImpl extends ResourceUtilization {

  private static final Log LOG = LogFactory
      .getLog(ResourceUtilizationPBImpl.class);

  private ResourceUtilizationProto proto = ResourceUtilizationProto
      .getDefaultInstance();
  private ResourceUtilizationProto.Builder builder = null;
  private boolean viaProto = false;

  TypedResourceUtilization[] resourceUtilizations = null;

  public ResourceUtilizationPBImpl() {
    builder = ResourceUtilizationProto.newBuilder();
  }

  public ResourceUtilizationPBImpl(ResourceUtilizationProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ResourceUtilizationProto getProto() {
    maybeInitBuilder();
    mergeLocalToBuilder();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    builder.clearTypedResources();
    if (resourceUtilizations != null && resourceUtilizations.length != 0) {
      ArrayList<TypedResourceUtilizationProto> list = new ArrayList<>();
      for (TypedResourceUtilization rtu : resourceUtilizations) {
        Objects.requireNonNull(rtu);
        TypedResourceUtilizationProto proto =
            ((TypedResourceUtilizationPBImpl)rtu).getProto();
        list.add(proto);
      }
      builder.addAllTypedResources(list);
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceUtilizationProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getPhysicalMemory() {
    ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getPmem());
  }

  @Override
  public void setPhysicalMemory(int pmem) {
    maybeInitBuilder();
    builder.setPmem(pmem);
  }

  @Override
  public int getVirtualMemory() {
    ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getVmem());
  }

  @Override
  public void setVirtualMemory(int vmem) {
    maybeInitBuilder();
    builder.setVmem(vmem);
  }

  @Override
  public float getCPU() {
    ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCpu();
  }

  @Override
  public void setCPU(float cpu) {
    maybeInitBuilder();
    builder.setCpu(cpu);
  }

  @Override
  public long getResourceValue(String name) {
    Objects.requireNonNull(name);
    initTypedResourceUtilization();
    Integer index = ResourceUtils.getResourceTypeIndex().get(name) - 2;
    if (index != null) {
      return resourceUtilizations[index].getLatestCapability().getValue();
    }
    throw new ResourceNotFoundException("Unknown resource:" + name);
  }

  @Override
  public void setResourceValue(String name, long value) {
    maybeInitBuilder();
    Objects.requireNonNull(name);
    initTypedResourceUtilization();
    LOG.debug("setResourceValue begin");
    Integer index = ResourceUtils.getResourceTypeIndex().get(name) - 2;
    if (index != null) {
      TypedResourceUtilization tru = resourceUtilizations[index];
      ResourceInformation ri =
          ResourceInformation.newInstance(tru.getLatestCapability());
      ri.setValue(value);
      resourceUtilizations[index].setLatestCapability(ri);
      LOG.debug("setResourceValue:" + resourceUtilizations[index]);
      return;
    }
    throw new ResourceNotFoundException("Unknown resource:" + name);
  }

  @Override
  public long getUsedResourceValue(String name) {
    Objects.requireNonNull(name);
    initTypedResourceUtilization();
    Integer index = ResourceUtils.getResourceTypeIndex().get(name) - 2;
    if (index != null) {
      return resourceUtilizations[index].getUsed();
    }
    throw new ResourceNotFoundException("Unknown resource:" + name);
  }

  @Override
  public void setUsedResourceValue(String name, long used) {
    maybeInitBuilder();
    Objects.requireNonNull(name);
    initTypedResourceUtilization();
    LOG.debug("setUsedResourceValue begin");
    Integer index = ResourceUtils.getResourceTypeIndex().get(name) - 2;
    if (index != null) {
      resourceUtilizations[index].setUsed(used);
      LOG.debug("setUsedResourceValue:" + resourceUtilizations[index]);
      return;
    }
    throw new ResourceNotFoundException("Unknown resource:" + name);
  }

  @Override
  public int compareTo(ResourceUtilization other) {
    int diff = this.getPhysicalMemory() - other.getPhysicalMemory();
    if (diff == 0) {
      diff = this.getVirtualMemory() - other.getVirtualMemory();
      if (diff == 0) {
        diff = Float.compare(this.getCPU(), other.getCPU());
      }
    }
    return diff;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  private void initTypedResourceUtilization() {
    if (this.resourceUtilizations != null) {
      return;
    }
    LOG.debug("Init typedResourceUtilization");
    ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
    ResourceInformation[] types = ResourceUtils.getResourceTypesArray();
    for (ResourceInformation ri : types) {
      LOG.debug("ResourceInformation:" + ri);
    }
    Map<String, Integer> indexMap = ResourceUtils.getResourceTypeIndex();
    // only store typed resource. Exclude CPU and memory
    if (types.length > 2) {
      this.resourceUtilizations =
          new TypedResourceUtilization[types.length - 2];
    }
    LOG.debug("TypedResourceUtilization array size:" + types.length);
    if (!viaProto) {
      LOG.debug("Try init typed resource utilization");
      for (int i = 0; i < types.length - 2; i++) {
        resourceUtilizations[i] =
            TypedResourceUtilization.newInstance(types[i + 2]
                , 0);
        LOG.debug("Inited one typed resource utilization "
            + this.resourceUtilizations[i]);
      }
      return;
    }
    LOG.debug("p.getTypedResourcesList().size:" + p.getTypedResourcesList().size());
    for (TypedResourceUtilizationProto entry : p.getTypedResourcesList()) {
      ResourceInformationProto riProto = entry.getCapability();
      if (riProto.hasKey()) {
        Integer index = indexMap.get(riProto.getKey()) - 2;
        if (index == null) {
          LOG.warn("Got unknown resource type: " + riProto.getKey() + "; skipping");
        } else {
          this.resourceUtilizations[index] =
              new TypedResourceUtilizationPBImpl(entry);
          LOG.debug(this.resourceUtilizations[index]);
        }
      }
    } // end for
  }
}
