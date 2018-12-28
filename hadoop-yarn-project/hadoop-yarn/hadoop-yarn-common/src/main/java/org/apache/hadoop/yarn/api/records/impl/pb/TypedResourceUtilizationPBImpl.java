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

package org.apache.hadoop.yarn.api.records.impl.pb;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.TypedResourceUtilization;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceInformationProto;
import org.apache.hadoop.yarn.proto.YarnProtos.TypedResourceUtilizationProto;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.HashSet;
import java.util.Objects;

/**
 * Wrapper class for generated TypedResourceUtilizationProto.
 * */
@Public
@Unstable
public class TypedResourceUtilizationPBImpl extends TypedResourceUtilization {

  private TypedResourceUtilizationProto.Builder builder;

  public TypedResourceUtilizationPBImpl() {
    this.builder = TypedResourceUtilizationProto.newBuilder();
  }

  public TypedResourceUtilizationPBImpl(TypedResourceUtilizationProto proto) {
    this.builder = proto.toBuilder();
  }

  @Override
  public ResourceInformation getLatestCapability() {
    Objects.requireNonNull(this.builder);
    return convertFromProto(this.builder.getCapability());
  }

  @Override
  public long getUsed() {
    Objects.requireNonNull(this.builder);
    return this.builder.getUsed();
  }

  @Override
  public void setLatestCapability(ResourceInformation newCap) {
    Objects.requireNonNull(this.builder);
    this.builder.clearCapability();
    this.builder.setCapability(convertToProto(newCap));
  }

  @Override
  public void setUsed(long count) {
    Objects.requireNonNull(this.builder);
    this.builder.setUsed(count);
  }

  private ResourceInformation convertFromProto(ResourceInformationProto riProto) {
    ResourceInformation ri = new ResourceInformation();
    ri.setName(riProto.getKey());
    ri.setResourceType(riProto.hasType()
        ? ProtoUtils.convertFromProtoFormat(riProto.getType())
        : ResourceTypes.COUNTABLE);
    String units = riProto.hasUnits() ? riProto.getUnits() :
        ResourceUtils.getDefaultUnit(riProto.getKey());
    long value = riProto.hasValue() ? riProto.getValue() : 0L;
    String destUnit = ResourceUtils.getDefaultUnit(riProto.getKey());
    if(!units.equals(destUnit)) {
      ri.setValue(UnitsConversionUtil.convert(units, destUnit, value));
      ri.setUnits(destUnit);
    } else {
      ri.setUnits(units);
      ri.setValue(value);
    }
    if (riProto.getTagsCount() > 0) {
      ri.setTags(new HashSet<>(riProto.getTagsList()));
    } else {
      ri.setTags(ImmutableSet.of());
    }
    if (riProto.getAttributesCount() > 0) {
      ri.setAttributes(ProtoUtils
          .convertStringStringMapProtoListToMap(riProto.getAttributesList()));
    } else {
      ri.setAttributes(ImmutableMap.of());
    }
    return ri;
  }

  private ResourceInformationProto convertToProto(ResourceInformation newCap) {
    ResourceInformationProto.Builder b = ResourceInformationProto.newBuilder();
    b.setKey(newCap.getName());
    b.setUnits(newCap.getUnits());
    b.setType(ProtoUtils.converToProtoFormat(newCap.getResourceType()));
    b.setValue(newCap.getValue());
    if (newCap.getAttributes() != null
        && !newCap.getAttributes().isEmpty()) {
      b.addAllAttributes(ProtoUtils.convertToProtoFormat(
          newCap.getAttributes()));
    }
    if (newCap.getTags() != null
        && !newCap.getTags().isEmpty()) {
      b.addAllTags(newCap.getTags());
    }
    return b.build();
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

  public TypedResourceUtilizationProto getProto() {
    Objects.requireNonNull(builder);
    return builder.build();
  }
}
