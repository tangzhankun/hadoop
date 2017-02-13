package org.apache.hadoop.yarn.api.records;

public class FPGASlot implements Comparable<FPGASlot>{

  private FPGAType fpgaType;
  private String slotId;
  private String afuId;

  public static FPGASlot newInstance(FPGAType type, String slotId, String afuId) {
    FPGASlot fpgaSlot = new FPGASlot();
    fpgaSlot.setFpgaType(type);
    fpgaSlot.setSlotId(slotId);
    fpgaSlot.setAfuId(afuId);
    return fpgaSlot;
  }

  public static FPGASlot newInstance(FPGASlot a) {
    FPGASlot fpgaSlot = new FPGASlot();
    fpgaSlot.setSlotId(a.getSlotId());
    fpgaSlot.setFpgaType(a.getFpgaType());
    fpgaSlot.setAfuId(a.getAfuId());
    return fpgaSlot;
  }

  public static FPGASlot newInstance() {
    FPGASlot fpgaSlot = new FPGASlot();
    return fpgaSlot;
  }

  public FPGAType getFpgaType() {
    return fpgaType;
  }

  public String getSlotId() {
    return slotId;
  }

  public String getAfuId() {
    return afuId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof FPGASlot))
      return false;
    FPGASlot other = (FPGASlot) obj;
    return !(getFpgaType() != other.getFpgaType()
        || !getAfuId().equals(other.getAfuId())
        || !getSlotId().equals(other.getSlotId()));
  }

  @Override
  public int hashCode() {
    final int prime = 263167;
    int result = 939769357 + fpgaType.hashCode();
    result = prime * result + slotId.hashCode();
    result = prime * result + afuId.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "fpga type: " + fpgaType + " slot id: " + slotId + " afu id: " + afuId + ".";
  }

  @Override
  public int compareTo(FPGASlot o) {
    int diff = this.fpgaType.compareTo(o.fpgaType);
    if (diff == 0) {
      diff = this.slotId.compareTo(o.slotId);
      if (diff == 0) {
        diff = this.afuId.compareTo(o.afuId);
      }
    }
    return diff;
  }

  public void setFpgaType(FPGAType fpgaType) {
    this.fpgaType = fpgaType;
  }

  public void setSlotId(String slotId) {
    this.slotId = slotId;
  }

  public void setAfuId(String afuId) {
    this.afuId = afuId;
  }
}