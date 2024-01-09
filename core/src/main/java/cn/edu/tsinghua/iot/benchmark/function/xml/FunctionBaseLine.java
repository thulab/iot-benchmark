package cn.edu.tsinghua.iot.benchmark.function.xml;

import cn.edu.tsinghua.iot.benchmark.utils.ReadWriteIOUtils;

import javax.xml.bind.annotation.XmlAttribute;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

public class FunctionBaseLine {
  private double lower;
  private double upper;
  private double ratio;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FunctionBaseLine that = (FunctionBaseLine) o;
    return Double.compare(lower, that.lower) == 0
        && Double.compare(upper, that.upper) == 0
        && Double.compare(ratio, that.ratio) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(lower, upper, ratio);
  }

  @XmlAttribute(name = "lower")
  public double getLower() {
    return lower;
  }

  public void setLower(double lower) {
    this.lower = lower;
  }

  @XmlAttribute(name = "upper")
  public double getUpper() {
    return upper;
  }

  public void setUpper(double upper) {
    this.upper = upper;
  }

  @Override
  public String toString() {
    return "FunctionBaseLine{" + "lower=" + lower + ", upper=" + upper + ", ratio=" + ratio + '}';
  }

  @XmlAttribute(name = "ratio")
  public double getRatio() {
    return ratio;
  }

  public void setRatio(double ratio) {
    this.ratio = ratio;
  }

  public void serialize(ByteArrayOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(lower, outputStream);
    ReadWriteIOUtils.write(upper, outputStream);
    ReadWriteIOUtils.write(ratio, outputStream);
  }

  public static FunctionBaseLine deserialize(ByteArrayInputStream inputStream) throws IOException {
    FunctionBaseLine result = new FunctionBaseLine();
    result.lower = ReadWriteIOUtils.readDouble(inputStream);
    result.upper = ReadWriteIOUtils.readDouble(inputStream);
    result.ratio = ReadWriteIOUtils.readDouble(inputStream);
    return result;
  }
}
