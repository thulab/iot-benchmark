package cn.edu.tsinghua.iotdb.benchmark.db.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Point;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class IoTDBTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBTest.class);
    //private static final String createSeriesSQL = "CREATE TIMESERIES %s WITH DATATYPE=%s,ENCODING=%s";
    private static final String createSeriesSQLWithCompressor = "CREATE TIMESERIES %s WITH DATATYPE=%s,ENCODING=%s,COMPRESSOR=%s";
    private static final String setStorageLevelSQL = "SET STORAGE GROUP TO %s";
    private Connection connection;
    private static Config config;
    private List<Point> points;
    private Map<String, String> mp;
    private long labID;
    private MySqlLog mySql;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Random sensorRandom;
    private Random timestampRandom;
    private ProbTool probTool;
    private final double unitTransfer = 1000000000.0;

    public IoTDBTest() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
        config = ConfigDescriptor.getInstance().getConfig();
        connection = DriverManager.getConnection(String.format(Constants.URL, config.host, config.port), Constants.USER,
                Constants.PASSWD);
    }

    public boolean testUserGuide() {
        boolean pass = true;
        pass = pass && testExecute("set storage group to root.ln", true);

        pass = pass && testExecute("set storage group to root.sgcc", true);



        return pass;
    }

    private boolean testExecuteQuery(String testSql, String expectedString) {
        boolean pass = true;
        Statement statement;
        ResultSet resultSet;
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return pass;
    }

    private boolean testExecute(String testSql, Boolean expectedExecuteReturn) {
        boolean pass;
        Statement statement;
        boolean executeReturn;
        try {
            statement = connection.createStatement();
            executeReturn = statement.execute(testSql);
            pass = (executeReturn == expectedExecuteReturn);
        } catch (SQLException e) {
            pass = false;
            e.printStackTrace();
        }
        return pass;
    }

    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        if (mySql != null) {
            mySql.closeMysql();
        }
    }

    static public void main(String[] args) throws SQLException {
        IoTDBTest ioTDB = null;
        try {
            ioTDB = new IoTDBTest();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Boolean userGuidePass;

        userGuidePass = ioTDB.testUserGuide();

        System.out.println(userGuidePass);
    }

}
