package cn.edu.tsinghua.iotdb.benchmark.tool;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportDataFromCSV {
	private Config config;
	private IotdbBasic iotdb;
	private static final Logger LOGGER = LoggerFactory.getLogger(ImportDataFromCSV.class);
	// storage csv table head info
	private List<String> headInfo = new ArrayList<>();
	// storage csv device sensor info, corresponding csv table head
	private Map<String, ArrayList<Integer>> deviceToColumn = new HashMap<>();
	// map column index to timederies path
	private List<String> colInfo = new ArrayList<>();
	// storage timeseries DataType
	private Map<String, String> timeseriesDataType = new HashMap<>();
	private static ThreadLocal<Long> totalTime = new ThreadLocal<Long>() {
		protected Long initialValue() {
			return (long) 0;
		}
	};
	private static ThreadLocal<Long> errorCount = new ThreadLocal<Long>() {
		protected Long initialValue() {
			return (long) 0;
		}
	};

	private static final String FILE_SUFFIX = "csv";

	public ImportDataFromCSV() {
		super();
		config = ConfigDescriptor.getInstance().getConfig();
		try {
			iotdb = new IotdbBasic();
		} catch (ClassNotFoundException | SQLException e) {
			LOGGER.error(e.getMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/** import file or the files in a directory into iotdb 
	 * @throws SQLException */
	public void importData(String path) throws SQLException {
		iotdb.init();
		File file = new File(path);

		if (file.isFile()) {
			if (file.getName().endsWith(FILE_SUFFIX)) {
				loadDataFromCSV(file, 1);
			} else {
				LOGGER.warn("File {} should ends with '.csv' if you want to import",
						file.getName());
				System.out.println("[WARN] File " + file.getName()
						+ " should ends with '.csv' if you want to import");
			}
		} else if (file.isDirectory()) {
			int i = 1;
			for (File f : file.listFiles()) {
				if (f.isFile()) {
					if (f.getName().endsWith(FILE_SUFFIX)) {
						loadDataFromCSV(f, i);
						i++;
					} else {
						LOGGER.warn("File {} should ends with '.csv' if you want to import",
								file.getName());
					}
				}
			}// for
		}
		iotdb.close();
	}

	/**
	 * Data from csv To IoTDB
	 */
	private void loadDataFromCSV(File file, int index) {
		LOGGER.info("From {} \n", file.getAbsolutePath());

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
			String line = "";
			String header = br.readLine();
			String[] strHeadInfo = header.split(",");
			LOGGER.info(header);
			if (strHeadInfo.length <= 1) {
				LOGGER.error("The CSV file illegal, please check first line",
						file.getAbsolutePath());
				System.out.println("[ERROR] The CSV file" + file.getName()
						+ " illegal, please check first line");
				return;
			}

			long startTime = System.currentTimeMillis();
			for (int i = 1; i < strHeadInfo.length; i++) {

				String type = iotdb.queryTimeseriesDataType(strHeadInfo[i]);
				if (type != null && type != "") {
					timeseriesDataType.put(strHeadInfo[i], type);
				} else {
					LOGGER.error("Database cannot find {} in {}, stop import!",
							strHeadInfo[i], file.getAbsolutePath());
					return;
				}
				headInfo.add(strHeadInfo[i]);
				String deviceInfo = strHeadInfo[i].substring(0,
						strHeadInfo[i].lastIndexOf("."));

				if (!deviceToColumn.containsKey(deviceInfo)) {
					deviceToColumn.put(deviceInfo, new ArrayList<>());
				}
				// storage every device's sensor index info
				deviceToColumn.get(deviceInfo).add(i - 1);
				colInfo.add(strHeadInfo[i].substring(strHeadInfo[i]
						.lastIndexOf(".") + 1));
			}

			int count = 0;
			List<String> tmp = new ArrayList<>();
			while ((line = br.readLine()) != null) {
				List<String> sqls = new ArrayList<>();
				try {
					sqls = SqlStatementBuilder.createInsertSQL(line,
							timeseriesDataType, deviceToColumn, colInfo,
							headInfo);
				} catch (Exception e) {
					LOGGER.error(
							"error input line, maybe it is not complete: {}",
							line);
				}
				for (String str : sqls) {
					count++;
					tmp.add(str);
					if (count == config.BATCH_EXECUTE_COUNT) {
						iotdb.insertOneBatch(tmp, totalTime, errorCount);
						count = 0;
						tmp.clear();
					}
				}
			}
			if (count > 0) {
				iotdb.insertOneBatch(tmp, totalTime, errorCount);
				count = 0;
				tmp.clear();
			}
			LOGGER.info(
					"Load data from {} successfully, it takes {}ms, insertData takes {}ms",
					file.getName(), (System.currentTimeMillis() - startTime),
					totalTime.get());

		} catch (FileNotFoundException e) {
			LOGGER.error("Cannot find {}", file.getName());
		} catch (IOException e) {
			LOGGER.error("CSV file read exception! {}", e.getMessage());
		} finally {
			if (br != null)
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					LOGGER.error("CSV file close exception! {}", e.getMessage());
					e.printStackTrace();
				}
			if (errorCount.get() > 0) {
				LOGGER.error(
						"Format of some lines in {} error, the error number is {}",
						file.getAbsolutePath(), errorCount.get());
			}
		}
	}

}
