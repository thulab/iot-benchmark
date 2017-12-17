package cn.edu.tsinghua.iotdb.benchmark.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.enums.LoadTypeEnum;

public class LoadRatio {
	private static final Logger LOGGER = LoggerFactory.getLogger(LoadRatio.class);
	private double writeEndRatio;
	private double randomInsertEndRatio;
	private double simpleQueryEndRatio;
	private double aggrQueryEndRatio;
	private double updateEndRatio;
	private double sumRatio;

	public LoadRatio(double writeRatio, double randomInsertRatio, double simpleQueryRatio, double aggrQueryRatio,
			double updateRatio) {
		super();
		if (writeRatio < 0 || randomInsertRatio < 0 || simpleQueryRatio < 0 || aggrQueryRatio < 0 || updateRatio < 0) {
			LOGGER.error("some ratio cannot less than 0, {}, {}, {}, {} ,{}",
					writeRatio, randomInsertRatio, simpleQueryRatio, aggrQueryRatio, updateRatio);
			System.exit(0);
		}
		sumRatio = writeRatio + randomInsertRatio + simpleQueryRatio + aggrQueryRatio + updateRatio;
		if (sumRatio < 0) {
			LOGGER.error("sum ratio cannot less than 0, {}", sumRatio);
			System.exit(0);
		}
		this.writeEndRatio = writeRatio / sumRatio;
		this.randomInsertEndRatio = writeEndRatio + (randomInsertRatio) / sumRatio;
		this.simpleQueryEndRatio = randomInsertEndRatio + (simpleQueryRatio) / sumRatio;
		this.aggrQueryEndRatio = simpleQueryEndRatio + (aggrQueryRatio) / sumRatio;
		this.updateEndRatio = aggrQueryEndRatio + (updateRatio) / sumRatio;
	}

	public static LoadRatio newInstanceByLoadType(LoadTypeEnum loadType) {
		Config config = ConfigDescriptor.getInstance().getConfig();
		switch (loadType) {
		case WRITE:
			return new LoadRatio(1, 0, 0, 0, 0);
		case RANDOM_INSERT:
			return new LoadRatio(0, 1, 0, 0, 0);
		case SIMPLE_READ:
			return new LoadRatio(0, 0, 1, 0, 0);
		case AGGRA_READ:
			return new LoadRatio(0, 0, 0, 1, 0);
		case UPDATE:
			return new LoadRatio(0, 0, 0, 0, 1);
		case MUILTI:
			return new LoadRatio(config.WRITE_RATIO, config.RANDOM_INSERT_RATIO, config.SIMPLE_QUERY_RATIO,
					config.MAX_QUERY_RATIO, config.UPDATE_RATIO);
		default:
			break;
		}
		LOGGER.error("loadType error {}", loadType);
		System.exit(0);
		return null;
	}
	
	public double getWriteEndRatio() {
		return writeEndRatio;
	}

	public double getRandomInsertEndRatio() {
		return randomInsertEndRatio;
	}

	public double getSimpleQueryEndRatio() {
		return simpleQueryEndRatio;
	}

	public double getAggrQueryEndRatio() {
		return aggrQueryEndRatio;
	}

	public double getUpdateEndRatio() {
		return updateEndRatio;
	}

	public double getWriteStartRatio() {
		return 0;
	}

	public double getRandomInsertStartRatio() {
		return writeEndRatio;
	}

	public double getSimpleQueryStartRatio() {
		return randomInsertEndRatio;
	}

	public double getAggrQueryStartRatio() {
		return simpleQueryEndRatio;
	}

	public double getUpdateStartRatio() {
		return aggrQueryEndRatio;
	}

}
