package org.apache.hadoop.yarn.api.records;

public class FPGAResource {

  private final String type;
  private final String accelerator;

  private FPGAResource(Builder builder) {
    this.type = builder.type;
    this.accelerator = builder.accelerator;
  }

  public String getType() {
    return type;
  }

  public String getAccelerator() {
    return accelerator;
  }

  @Override
  public String toString() {
    return "fpga type: " + type + " fpga accelerator: " + accelerator + ".";
  }

  public static class Builder {

    private String type;
    private String accelerator;

    public Builder type(String type) {
      this.type = type;
      return this;
    }

    public Builder accelerator(String accelerator) {
      this.accelerator = accelerator;
      return this;
    }

    synchronized public FPGAResource build() {
      return new FPGAResource(this);
    }

  }

}