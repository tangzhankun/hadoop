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
import java.util.List;

@Private
@Unstable
public class ResourcePBImpl extends Resource {
  ResourceProto proto = ResourceProto.getDefaultInstance();
  ResourceProto.Builder builder = null;
  boolean viaProto = false;
  
  public ResourcePBImpl() {
    builder = ResourceProto.newBuilder();
  }

  public ResourcePBImpl(ResourceProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ResourceProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceProto.newBuilder(proto);
    }
    viaProto = false;
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
  public List<FPGASlot> getFPGASlots() {
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    List<FPGAOptionProto> fpgaSlotsProtoList = p.getFpgaSlotsList();
    return convertToFPGASlotList(fpgaSlotsProtoList);
  }

  @Override
  public void setFPGASlots(List<FPGASlot> fpgaSlots) {
    maybeInitBuilder();
    builder.addAllFpgaSlots(convertToFPGAProtoList(fpgaSlots));
  }

  private List<FPGAOptionProto> convertToFPGAProtoList(List<FPGASlot> fpgaSlots) {
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

  private List<FPGASlot> convertToFPGASlotList(List<FPGAOptionProto> fpgaSlotsProtoList) {
    List<FPGASlot> fpgaSlotList = new ArrayList<FPGASlot>();
    FPGASlot.Builder builder = new FPGASlot.Builder();
    for(FPGAOptionProto proto : fpgaSlotsProtoList) {
      FPGASlot fpgaSlot = builder.fpgaType(FPGAType.valueOf(proto.getFpgaType().name()))
              .socketId(proto.getSocketId())
              .slotId(proto.getSlotId())
              .afuId(proto.getAfuId()).build();
      fpgaSlotList.add(fpgaSlot);
    }
    return fpgaSlotList;
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
