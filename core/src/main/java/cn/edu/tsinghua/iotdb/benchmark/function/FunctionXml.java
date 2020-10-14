package cn.edu.tsinghua.iotdb.benchmark.function;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 用于解析function.xml
 */
@XmlRootElement(name = "functions")
public class FunctionXml {
	private List<FunctionParam> functions = new ArrayList<FunctionParam>();

	@XmlElement(name = "function")
	public List<FunctionParam> getFunctions() {
		return functions;
	}

	public void setFunctions(List<FunctionParam> functions) {
		this.functions = functions;
	}

	public static void main(String[] args) {

	}
}
