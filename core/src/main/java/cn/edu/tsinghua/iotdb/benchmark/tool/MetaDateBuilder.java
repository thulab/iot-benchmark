package cn.edu.tsinghua.iotdb.benchmark.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaDateBuilder {
	private Set<String> storageGroups;
	private Map<String, Integer> colInfo;
	private IotdbBasic iotdb;
	private static final Logger LOGGER = LoggerFactory.getLogger(MetaDateBuilder.class);

	private static final String PATH = "path";
	private static final String STORAGE_GROUP = "storageGroup";
	private static final String TYPE = "type";
	private static final String ENCODING = "encoding";
	private static final String FILE_SUFFIX = "csv";

	public MetaDateBuilder() {
		storageGroups = new HashSet<String>();
		colInfo = new HashMap<String, Integer>();
		try {
			iotdb = new IotdbBasic();
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}
	}

	public void createMataData(String path) throws SQLException {
		if(path == null || path == ""){
			return ;
		}
		iotdb.init();
		File file = new File(path);
		if (file.isFile()) {
			if (file.getName().endsWith(FILE_SUFFIX)) {
				createMataDataFromCSV(file, 1);
			} else {
				LOGGER.warn(
						"metaDataFile {} should ends with '.csv' if you want to import",
						file.getName());
			}
		} else if (file.isDirectory()) {
			int i = 1;
			for (File f : file.listFiles()) {
				if (f.isFile()) {
					if (f.getName().endsWith(FILE_SUFFIX)) {
						createMataDataFromCSV(f, i);
						i++;
					} else {
						LOGGER.warn(
								"File {} should ends with '.csv' if you want to import",
								file.getName());
					}
				}
			}// for
		}
		iotdb.close();
	}

	private void createMataDataFromCSV(File file, int i) {
		// TODO Auto-generated method stub
		LOGGER.info("From {} \n", file.getAbsolutePath());

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));

			// 处理文件第一行的列信息
			String header = br.readLine();
			LOGGER.info(header);
			if (!parseFileFirstLine(header)) {
				LOGGER.error("The CSV file {} illegal, please check first line. {}",
						file.getAbsolutePath(), header);
				return;
			}

			// 处理元数据文件的数据
			String line = "";
			int limit = colInfo.size() + 1;
			while ((line = br.readLine()) != null) {
				String tmp[] = line.split(",", limit);
				String storageGroupTmp = tmp[colInfo.get(STORAGE_GROUP)];
				if (!isStorageGroupExist(storageGroupTmp)) {
					storageGroups.add(storageGroupTmp);
					iotdb.setStorgeGroup(storageGroupTmp);
				}

				iotdb.createTimeseries(tmp[colInfo.get(PATH)],
						tmp[colInfo.get(TYPE)], tmp[colInfo.get(ENCODING)]);
			}

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
		}
	}

	/**
	 * 解析CSV文件的文件第一行获得列信息 若不符合规范返回false，符合规范返回true
	 * */
	private boolean parseFileFirstLine(String line) {
		colInfo.clear();
		String[] strHeadInfo = line.split(",");
		for (int i = 0; i < strHeadInfo.length; i++) {
			String str = strHeadInfo[i];
			if (str.equalsIgnoreCase(PATH)) {
				colInfo.put(PATH, i);
			} else if (str.equalsIgnoreCase(STORAGE_GROUP)) {
				colInfo.put(STORAGE_GROUP, i);
			} else if (str.equalsIgnoreCase(TYPE)) {
				colInfo.put(TYPE, i);
			} else if (str.equalsIgnoreCase(ENCODING)) {
				colInfo.put(ENCODING, i);
			}
		}
		if (colInfo.size() < 4) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * 检测存储组是否存在 （目前根据set集合判断，以后可能改为通过数据库查询得到）
	 * */
	private boolean isStorageGroupExist(String path) {
		if (path == null || path == "" || storageGroups.contains(path)) {
			return true;
		} else {
			return false;
		}
	}

}
