package cn.edu.tsinghua.iotdb.benchmark.function;

import javax.xml.bind.annotation.XmlAttribute;

/**
 * 函数参数类 主要包含函数类别，函数参数
 * 
 */
public class FunctionParam {
	private String id;
	private String functionType;// 函数类别
	private double max;// 最大值
	private double min;// 最小值
	private long cycle;// 周期,(对于 *-k函数，则只是为了计算斜率)

	@XmlAttribute(name = "function-type")
	public String getFunctionType() {
		return functionType;
	}

	public void setFunctionType(String functionType) {
		this.functionType = functionType;
	}

	@XmlAttribute(name = "max")
	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	@XmlAttribute(name = "min")
	public double getMin() {
		return min;
	}

	public void setMin(double min) {
		this.min = min;
	}

	@XmlAttribute(name = "cycle")
	public long getCycle() {
		return cycle;
	}

	public void setCycle(long cycle) {
		this.cycle = cycle;
	}

	public FunctionParam(String functionType, double max, double min, long cycle) {
		super();
		this.functionType = functionType;
		this.max = max;
		this.min = min;
		this.cycle = cycle;
	}

	public FunctionParam() {
		super();
	}

	@XmlAttribute(name = "id")
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "FunctionParam [id=" + id + ", functionType=" + functionType + ", max=" + max + ", min=" + min
				+ ", cycle=" + cycle + "]";
	}

	@Override
	public boolean equals(Object obj) {
		if (getId().equals(((FunctionParam) obj).getId())) {
			return true;
		}
		return super.equals(obj);
	}

}
