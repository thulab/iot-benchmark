package cn.edu.tsinghua.iotdb.benchmark.function;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.enums.FunctionType;
import java.util.Random;

public class Function {
//	private final static double[] ABNORMAL_RATE = { 0.005, 0.01, 0.1, 0.15, 0.2 };
//	private final static long RELATIVE_ZERO_TIME = TimeUtils.convertDateStrToTimestamp("2016-01-13 00:00:00");
//
//	/**
//	 * 获取带有噪点的值,并且带浮动值，上下浮动value*0.005
//	 * 
//	 * @param value
//	 * @return
//	 */
//	public static double getAbnormalPoint(double value) {
//		value = value * (1 + (RANDOM.nextDouble() / 100 - 0.005));
//		if (RANDOM.nextDouble() < ABNORMAL_RATE[0]) {
//			value = value * (1 + (RANDOM.nextDouble() - 0.5));
//		}
//		return value;
//	}
	private static Config config = ConfigDescriptor.getInstance().getConfig();
	private static Random r = new Random(config.DATA_SEED);



	/**
	 * 获取单调函数浮点值
	 * 
	 * @param max 最大值
	 * @param min 最小值
	 * @param cycle 周期，单位为ms
	 * @param currentTime 当前时间 单位为ms
	 * @return
	 */
	private static double getMonoValue(double max, double min, double cycle, long currentTime) {
		double k = (max - min) / cycle;
		return min + k * (currentTime % cycle);
	}

	/**
	 * 获取单调函数浮点值
	 * 
	 * @param max 最大值
	 * @param min 最小值
	 * @param cycle 周期，单位为ms
	 * @param currentTime 当前时间 单位为ms
	 * @return
	 */
	private static double getMonoKValue(double max, double min, double cycle, long currentTime) {
		double k = (max - min) / (cycle);
		return min + k * (currentTime % cycle);
	}

//	/**
//	 * 
//	 * @param max 最大值
//	 * @param min 最小值
//	 * @param cycle 周期，单位为s
//	 * @param currentTime 当前时间 单位为ms
//	 * @return
//	 */
//	private static long getMonoValue(long max, long min, double cycle, long currentTime) {
//		double k = (max - min) / (cycle * 1000);
//		return (long) (k * (currentTime % (cycle * 1000)));
//	}

	/**
	 * 获取正弦函数浮点值
	 * 
	 * @param max 最大值
	 * @param min 最小值
	 * @param cycle 周期，单位为s
	 * @param currentTime 当前时间 单位为ms
	 * @return
	 */
	private static double getSineValue(double max, double min, double cycle, long currentTime) {
		double w = 2 * Math.PI / (cycle * 1000);
		double a = (max - min) / 2;
		double b = (max - min) / 2;
		return Math.sin(w * (currentTime % (cycle * 1000))) * a + b + min;
	}

	/**
	 * 获取方波函数浮点值
	 * 
	 * @param max 最大值
	 * @param min 最小值
	 * @param cycle 周期，单位为ms
	 * @param currentTime 当前时间 单位为ms
	 * @return
	 */
	private static double getSquareValue(double max, double min, double cycle, long currentTime) {
		double t = cycle / 2 ;
		if ((currentTime % (cycle)) < t) {
			return max;
		} else {
			return min;
		}
	}

	/**
	 * 获取随机数函数浮点值
	 * 
	 * @param max 最大值
	 * @param min 最小值
	 * @return
	 */
	private static double getRandomValue(double max, double min) {
		return r.nextDouble() * (max - min) + min;
	}
	
	

	public static void main(String[] args) throws InterruptedException {

	}
	
	public static Number getValueByFuntionidAndParam(FunctionParam param, long currentTime) {
		return getValueByFuntionidAndParam(FunctionType.valueOf(
				param.getFunctionType().toUpperCase()), param.getMax(), param.getMin(), param.getCycle(), currentTime);
	}

	public static Number getValueByFuntionidAndParam(FunctionType functionType, double max, double min, long cycle,
			long currentTime) {
		switch (functionType) {
			case FLOAT_MONO:
				return (float) getMonoValue(max, min, cycle, currentTime);
			case FLOAT_SIN:
				return (float) getSineValue(max, min, cycle, currentTime);
			case FLOAT_RANDOM:
				return (float) getRandomValue(max, min);
			case FLOAT_SQUARE:
				return (float) getSquareValue(max, min, cycle, currentTime);
			case FLOAT_MONO_K:
				return (float) getMonoKValue(max, min, cycle, currentTime);
			case DOUBLE_MONO:
				return getMonoValue(max, min, cycle, currentTime);
			case DOUBLE_SIN:
				return getSineValue(max, min, cycle, currentTime);
			case DOUBLE_RANDOM:
				return getRandomValue(max, min);
			case DOUBLE_SQUARE:
				return getSquareValue(max, min, cycle, currentTime);
			case DOUBLE_MONO_K:
				return getMonoKValue(max, min, cycle, currentTime);
			case INT_MONO:
				return (int) getMonoValue(max, min, cycle, currentTime);
			case INT_SIN:
				return (int) getSineValue(max, min, cycle, currentTime);
			case INT_RANDOM:
				return (int) getRandomValue(max, min);
			case INT_SQUARE:
				return (int) getSquareValue(max, min, cycle, currentTime);
			case INT_MONO_K:
				return (int) getMonoKValue(max, min, cycle, currentTime);			
			default:
				return 0;
		}
	}
}
