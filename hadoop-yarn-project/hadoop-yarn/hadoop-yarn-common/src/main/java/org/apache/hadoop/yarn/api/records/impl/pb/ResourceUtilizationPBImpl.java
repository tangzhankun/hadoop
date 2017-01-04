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
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceUtilizationProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceUtilizationProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.FPGAOptionProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FPGATypeProto;

import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

@Private
@Unstable
public class ResourceUtilizationPBImpl extends ResourceUtilization {
  private ResourceUtilizationProto proto = ResourceUtilizationProto
      .getDefaultInstance();
  private ResourceUtilizationProto.Builder builder = null;
  private boolean viaProto = false;
  Set<FPGASlot> fpgaSlots = null;

  public ResourceUtilizationPBImpl() {
    builder = ResourceUtilizationProto.newBuilder();
    initFpgaSlots();
  }

  public ResourceUtilizationPBImpl(ResourceUtilizationProto proto) {
    this.proto = proto;
    viaProto = true;
    initFpgaSlots();
  }

  public ResourceUtilizationProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceUtilizationProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void initFpgaSlots() {
    if(this.fpgaSlots == null) {
      fpgaSlots = new HashSet<FPGASlot>();
      FPGASlot.Builder builder = new FPGASlot.Builder();
      fpgaSlots.add(builder.fpgaType(FPGAType.ANY).socketId("0").slotId("0").afuId("00000000-0000-0000-0000-000000000000").build());
    }
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
  public Set<FPGASlot> getFPGASlots() {
        ResourceUtilizationProtoOrBuilder p = viaProto ? proto : builder;
        List<FPGAOptionProto> fpgaSlotsProtoList = p.getFpgaSlotsList();
        return convertToFPGASlots(fpgaSlotsProtoList);
      }

  @Override
  public void setFPGASlots(Set<FPGASlot> fpgaSlots) {
    maybeInitBuilder();
    builder.addAllFpgaSlots(convertToFPGAProtos(fpgaSlots));
  }

  private List<FPGAOptionProto> convertToFPGAProtos(Set<FPGASlot> fpgaSlots) {
    List<FPGAOptionProto> fpgaProtos = new ArrayList<FPGAOptionProto>();
    FPGAOptionProto.Builder fpgaProtoBuilder = FPGAOptionProto.newBuilder();
    for(FPGASlot fpgaSlot : fpgaSlots) {
      fpgaProtoBuilder.setFpgaType(FPGATypeProto.valueOf(fpgaSlot.getFpgaType().name()));
      fpgaProtoBuilder.setSocketId(fpgaSlot.getSocketId());
      fpgaProtoBuilder.setSlotId(fpgaSlot.getSlotId());
      fpgaProtoBuilder.setAfuId(fpgaSlot.getAfuId());
      FPGAOptionProto fpgaProto = fpgaProtoBuilder.build();
      fpgaProtos.add(fpgaProto);
    }
    return fpgaProtos;
  }

  private Set<FPGASlot> convertToFPGASlots(List<FPGAOptionProto> fpgaSlotsProtoList) {
    Set<FPGASlot> fpgaSlots = new HashSet<FPGASlot>();
    FPGASlot.Builder builder = new FPGASlot.Builder();
    for(FPGAOptionProto proto : fpgaSlotsProtoList) {
      FPGASlot fpgaSlot = builder.fpgaType(FPGAType.valueOf(proto.getFpgaType().name()))
              .socketId(proto.getSocketId())
              .slotId(proto.getSlotId())
              .afuId(proto.getAfuId()).build();
      fpgaSlots.add(fpgaSlot);
    }
    return fpgaSlots;
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
}
