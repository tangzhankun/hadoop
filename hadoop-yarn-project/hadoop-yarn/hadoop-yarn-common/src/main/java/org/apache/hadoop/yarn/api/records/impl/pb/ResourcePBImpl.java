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


import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.FPGASlot;
import org.apache.hadoop.yarn.api.records.FPGAType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FPGAOptionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FPGATypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProtoOrBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Private
@Unstable
public class ResourcePBImpl extends Resource {
  ResourceProto proto = ResourceProto.getDefaultInstance();
  ResourceProto.Builder builder = null;
  boolean viaProto = false;
  Set<FPGASlot> fpgaSlots;
  
  public ResourcePBImpl() {
    builder = ResourceProto.newBuilder();
  }

  public ResourcePBImpl(ResourceProto proto) {
    this.proto = proto;
    viaProto = true;
    this.fpgaSlots = null;
    initFpgaSlots();
  }
  
  public ResourceProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  synchronized private void mergeLocalToBuilder() {
    builder.clearFpgaSlots();
    if (fpgaSlots != null && !fpgaSlots.isEmpty()) {
      for (FPGASlot fpgaSlot : fpgaSlots) {
        FPGAOptionProto.Builder b = FPGAOptionProto.newBuilder();
        b.setFpgaType(FPGATypeProto.valueOf(fpgaSlot.getFpgaType().name()));
        b.setSlotId(fpgaSlot.getSlotId());
        b.setAfuId(fpgaSlot.getAfuId());
        builder.addFpgaSlots(b);
      }
    }
    builder.setMemory(this.getMemorySize());
    builder.setVirtualCores(this.getVirtualCores());
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void initFpgaSlots() {
    if (this.fpgaSlots != null) {
      return;
    }
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    fpgaSlots = new HashSet<FPGASlot>();
    for (FPGAOptionProto fpgaProto : p.getFpgaSlotsList()) {
      this.fpgaSlots.add(FPGASlot.newInstance(
          FPGAType.valueOf(fpgaProto.getFpgaType().name()),
          fpgaProto.getSlotId(),
          fpgaProto.getAfuId()));
    }
    if (this.getMemory() != p.getMemory()) {
      setMemorySize(p.getMemory());
    }
    if (this.getVirtualCores() != p.getVirtualCores()) {
      setVirtualCores(p.getVirtualCores());
    }

  }

  @Override
  @SuppressWarnings("deprecation")
  public int getMemory() {
    return (int) getMemorySize();
  }

  @Override
  public long getMemorySize() {
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMemory();
  }

  @Override
  @SuppressWarnings("deprecation")
  public void setMemory(int memory) {
    setMemorySize(memory);
  }

  @Override
  public void setMemorySize(long memory) {
    maybeInitBuilder();
    builder.setMemory(memory);
  }

  @Override
  public int getVirtualCores() {
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    return p.getVirtualCores();
  }

  @Override
  public void setVirtualCores(int vCores) {
    maybeInitBuilder();
    builder.setVirtualCores(vCores);
  }

  @Override
  public Set<FPGASlot> getFPGASlots() {
    initFpgaSlots();
    return this.fpgaSlots;
  }

  @Override
  public void setFPGASlots(Set<FPGASlot> fpgaSlots) {
    maybeInitBuilder();
    if (fpgaSlots == null || fpgaSlots.isEmpty()) {
      builder.clearFpgaSlots();
    }
    this.fpgaSlots = fpgaSlots;
  }

  @Override
  public int compareTo(Resource other) {
    long diff = this.getMemorySize() - other.getMemorySize();
    if (diff == 0) {
      diff = this.getVirtualCores() - other.getVirtualCores();
    }
    return diff == 0 ? 0 : (diff > 0 ? 1 : -1);
  }
  
  
}  
