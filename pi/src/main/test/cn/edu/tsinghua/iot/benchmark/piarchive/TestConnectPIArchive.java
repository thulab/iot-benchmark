package cn.edu.tsinghua.iot.benchmark.piarchive;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class TestConnectPIArchive {
    final String host = "PI";
    final String dataSource = "PI";
    Connection connection = null;
    String driverClassName = "com.osisoft.jdbc.Driver";
    PreparedStatement pStatement = null;
    ResultSet resultSet = null;
    Properties properties = new Properties();
    String url = "jdbc:pioledb://%s/Data Source=%s; Integrated Security=SSPI";
    String pointName = "test";
    String pointtypex = "float32";

    @Before
    public void init() {
        properties.put("TrustedConnection", "yes");
        properties.put("ProtocolOrder", "nettcp:5462");
        properties.put("LogConsole", "True");
        properties.put("LogLevel", "2");

        try {
            Class.forName(driverClassName).newInstance();
            connection = DriverManager.getConnection(
                    String.format(
                            url,
                            host,
                            dataSource
                    ), properties
            );
        } catch (Exception ex) {
            System.err.println(ex);
        }
    }

    @Test
    public void testCreatePoints() {
        try {
            pStatement = connection.prepareStatement("INSERT pipoint..classic (tag, pointtypex, compressing) VALUES (?, ?, 0)");
            for (int i = 0; i < 100; i++) {
                pStatement.setString(1, pointName + i);
                pStatement.setString(2, pointtypex);
                pStatement.addBatch();
            }
            pStatement.executeBatch();
            connection.commit();
            pStatement.clearBatch();
            pStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInsertRecord() {
        try {
            pStatement = connection.prepareStatement("insert piarchive..picomp (tag, value, time) values ('g_testTag1', ?, ?)");
//            pStatement = connection.prepareStatement("insert piarchive..picomp (tag, value) values ('g_testTag', ?)");
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            for (Long i = 0L; i < 50000L; i++) {
                pStatement.setString(1, String.valueOf(i));
                pStatement.setString(2, formatter.format(1432451409000L + i * 1000));
                pStatement.addBatch();
            }

            pStatement.executeBatch();
            connection.commit();
            pStatement.clearBatch();
            pStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void close() throws SQLException {
        if (resultSet != null) {
            resultSet.close();
        }
        if (connection != null) {
            connection.close();
        }

    }
}
