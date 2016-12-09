package org.apache.hadoop.yarn.api.records;

public class FPGASlot {

  private final FPGAType fpgaType;
  private final String socketId;
  private final String slotId;
  private final String afuId;

  private FPGASlot(Builder builder) {
    this.fpgaType = builder.fpgaType;
    this.socketId = builder.socketId;
    this.slotId = builder.slotId;
    this.afuId = builder.afuId;
  }

  public FPGAType getFpgaType() {
    return fpgaType;
  }

  public String getSocketId() {
    return socketId;
  }

  public String getSlotId() {
    return slotId;
  }

  public String getAfuId() {
    return afuId;
  }

  @Override
  public String toString() {
    return "fpga type: " + fpgaType + " socket id: " + socketId + " slot id: " + slotId + " afu id: " + afuId + ".";
  }

  public static class Builder {

    private FPGAType fpgaType;
    private String socketId;
    private String slotId;
    private String afuId;

    public Builder fpgaType(FPGAType fpgaType) {
      this.fpgaType = fpgaType;
      return this;
    }

    public Builder socketId(String socketId) {
      this.socketId = socketId;
      return this;
    }

    public Builder slotId(String slotId) {
      this.slotId = slotId;
      return this;
    }

    public Builder afuId(String afuId) {
      this.afuId = afuId;
      return this;
    }

    synchronized public FPGASlot build() {
      return new FPGASlot(this);
    }

  }

}