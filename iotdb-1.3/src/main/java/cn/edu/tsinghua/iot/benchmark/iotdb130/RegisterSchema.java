package cn.edu.tsinghua.iot.benchmark.iotdb130;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.utils.TimeUtils;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public abstract class RegisterSchema implements IDatabase {

    private static final String ALREADY_KEYWORD = "already";
    private static final AtomicBoolean templateInit = new AtomicBoolean(false);
    protected final Logger LOGGER;
    protected SingleNodeJDBCConnection ioTDBConnection;

    protected static final Config config = ConfigDescriptor.getInstance().getConfig();
    protected static final CyclicBarrier templateBarrier =
            new CyclicBarrier(config.getCLIENT_NUMBER());
    protected static final CyclicBarrier schemaBarrier = new CyclicBarrier(config.getCLIENT_NUMBER());
    protected static final CyclicBarrier activateTemplateBarrier =
            new CyclicBarrier(config.getCLIENT_NUMBER());
    protected static Set<String> storageGroups = Collections.synchronizedSet(new HashSet<>());
    protected final String ROOT_SERIES_NAME;
    protected ExecutorService service;
    protected Future<?> task;
    protected DBConfig dbConfig;
    protected Random random = new Random(config.getDATA_SEED());

    public RegisterSchema(DBConfig dbConfig, Class<?> clazz) {
        this.dbConfig = dbConfig;
        this.ROOT_SERIES_NAME = initializeRootSeriesName(dbConfig);
        this.LOGGER = LoggerFactory.getLogger(clazz);
    }
    protected abstract String initializeRootSeriesName(DBConfig dbConfig);

    @Override
    public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
        // create timeseries one by one is too slow in current cluster server.
        // therefore, we use session to create time series in batch.
        long start = System.nanoTime();
        long end;
        if (config.hasWrite()) {
            Map<Session, List<TimeseriesSchema>> sessionListMap = new HashMap<>();
            try {
                // open meta session
                if (!config.isIS_ALL_NODES_VISIBLE()) {
                    Session metaSession =
                            new Session.Builder()
                                    .host(dbConfig.getHOST().get(0))
                                    .port(Integer.parseInt(dbConfig.getPORT().get(0)))
                                    .username(dbConfig.getUSERNAME())
                                    .password(dbConfig.getPASSWORD())
                                    .version(Version.V_1_0)
                                    .build();
                    metaSession.open(config.isENABLE_THRIFT_COMPRESSION());
                    sessionListMap.put(metaSession, createTimeseries(schemaList));
                } else {
                    int sessionNumber = dbConfig.getHOST().size();
                    List<Session> keys = new ArrayList<>();
                    for (int i = 0; i < sessionNumber; i++) {
                        Session metaSession =
                                new Session.Builder()
                                        .host(dbConfig.getHOST().get(i))
                                        .port(Integer.parseInt(dbConfig.getPORT().get(i)))
                                        .username(dbConfig.getUSERNAME())
                                        .password(dbConfig.getPASSWORD())
                                        .version(Version.V_1_0)
                                        .build();
                        metaSession.open(config.isENABLE_THRIFT_COMPRESSION());
                        keys.add(metaSession);
                        sessionListMap.put(metaSession, new ArrayList<>());
                    }
                    for (int i = 0; i < schemaList.size(); i++) {
                        sessionListMap
                                .get(keys.get(i % sessionNumber))
                                .add(createTimeseries(schemaList.get(i)));
                    }
                }

                if (config.isTEMPLATE() && templateInit.compareAndSet(false, true)) {
                    Template template = null;
                    if (config.isTEMPLATE() && schemaList.size() > 0) {
                        template = createTemplate(schemaList.get(0));
                    }
                    start = System.nanoTime();
                    int sessionIndex = random.nextInt(sessionListMap.size());
                    Session templateSession = new ArrayList<>(sessionListMap.keySet()).get(sessionIndex);
                    registerTemplate(templateSession, template);
                } else {
                    start = System.nanoTime();
                }
                templateBarrier.await();
                for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
                    registerStorageGroups(pair.getKey(), pair.getValue());
                }
                schemaBarrier.await();
                if (config.isTEMPLATE()) {
                    for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
                        activateTemplate(pair.getKey(), pair.getValue());
                    }
                    activateTemplateBarrier.await();
                }
                if (!config.isTEMPLATE()) {
                    for (Map.Entry<Session, List<TimeseriesSchema>> pair : sessionListMap.entrySet()) {
                        registerTimeseries(pair.getKey(), pair.getValue());
                    }
                }
            } catch (Exception e) {
                throw new TsdbException(e);
            } finally {
                if (sessionListMap.size() != 0) {
                    Set<Session> sessions = sessionListMap.keySet();
                    for (Session session : sessions) {
                        try {
                            session.close();
                        } catch (IoTDBConnectionException e) {
                            LOGGER.error("Schema-register session cannot be closed: {}", e.getMessage());
                        }
                    }
                }
            }
        }
        end = System.nanoTime();
        return TimeUtils.convertToSeconds(end - start, "ns");
    }

    private Template createTemplate(DeviceSchema deviceSchema) {
        Template template = null;
        if (config.isTEMPLATE()) {
            if (config.isVECTOR()) {
                template = new Template(config.getTEMPLATE_NAME(), true);
            } else {
                template = new Template(config.getTEMPLATE_NAME(), false);
            }
            try {
                for (Sensor sensor : deviceSchema.getSensors()) {
                    MeasurementNode measurementNode =
                            new MeasurementNode(
                                    sensor.getName(),
                                    Enum.valueOf(TSDataType.class, sensor.getSensorType().name),
                                    Enum.valueOf(TSEncoding.class, getEncodingType(sensor.getSensorType())),
                                    Enum.valueOf(CompressionType.class, config.getCOMPRESSOR()));
                    template.addToTemplate(measurementNode);
                }
            } catch (StatementExecutionException e) {
                LOGGER.error(e.getMessage());
                return null;
            }
        }
        return template;
    }

    /** register template */
    private void registerTemplate(Session metaSession, Template template)
            throws IoTDBConnectionException, IOException {
        try {
            metaSession.createSchemaTemplate(template);
        } catch (StatementExecutionException e) {
            // do nothing
            e.printStackTrace();
        }
    }

    private void registerStorageGroups(Session metaSession, List<TimeseriesSchema> schemaList)
            throws TsdbException {
        // get all storage groups
        Set<String> groups = new HashSet<>();
        for (TimeseriesSchema timeseriesSchema : schemaList) {
            DeviceSchema schema = timeseriesSchema.getDeviceSchema();
            synchronized (IoTDB.class) {
                if (!storageGroups.contains(schema.getGroup())) {
                    groups.add(schema.getGroup());
                    storageGroups.add(schema.getGroup());
                }
            }
        }
        // register storage groups
        for (String group : groups) {
            try {
                metaSession.setStorageGroup(ROOT_SERIES_NAME + "." + group);
                if (config.isTEMPLATE()) {
                    metaSession.setSchemaTemplate(config.getTEMPLATE_NAME(), ROOT_SERIES_NAME + "." + group);
                }
            } catch (Exception e) {
                handleRegisterException(e);
            }
        }
    }

    private void handleRegisterException(Exception e) throws TsdbException {
        // ignore if already has the time series
        if (!e.getMessage().contains(ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
            LOGGER.error("Register IoTDB schema failed because ", e);
            throw new TsdbException(e);
        }
    }

    private void activateTemplate(Session metaSession, List<TimeseriesSchema> schemaList) {
        try {
            List<String> devicePaths =
                    schemaList.stream()
                            .map(schema -> ROOT_SERIES_NAME + "." + schema.getDeviceSchema().getDevicePath())
                            .collect(Collectors.toList());
            metaSession.createTimeseriesUsingSchemaTemplate(devicePaths);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private TimeseriesSchema createTimeseries(DeviceSchema deviceSchema) {
        List<String> paths = new ArrayList<>();
        List<TSDataType> tsDataTypes = new ArrayList<>();
        List<TSEncoding> tsEncodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        for (Sensor sensor : deviceSchema.getSensors()) {
            if (config.isVECTOR()) {
                paths.add(sensor.getName());
            } else {
                paths.add(getSensorPath(deviceSchema, sensor.getName()));
            }
            SensorType datatype = sensor.getSensorType();
            tsDataTypes.add(Enum.valueOf(TSDataType.class, datatype.name));
            tsEncodings.add(Enum.valueOf(TSEncoding.class, getEncodingType(datatype)));
            compressionTypes.add(Enum.valueOf(CompressionType.class, config.getCOMPRESSOR()));
        }
        TimeseriesSchema timeseriesSchema =
                new TimeseriesSchema(deviceSchema, paths, tsDataTypes, tsEncodings, compressionTypes);
        if (config.isVECTOR()) {
            timeseriesSchema.setDeviceId(getDevicePath(deviceSchema));
        }
        return timeseriesSchema;
    }


    private String getSensorPath(DeviceSchema deviceSchema, String sensor) {
        return getDevicePath(deviceSchema) + "." + sensor;
    }

    protected String getDevicePath(DeviceSchema deviceSchema) {
        StringBuilder name = new StringBuilder(ROOT_SERIES_NAME);
        name.append(".").append(deviceSchema.getGroup());
        for (Map.Entry<String, String> pair : deviceSchema.getTags().entrySet()) {
            name.append(".").append(pair.getValue());
        }
        name.append(".").append(deviceSchema.getDevice());
        return name.toString();
    }
    private List<TimeseriesSchema> createTimeseries(List<DeviceSchema> schemaList) {
        List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
        for (DeviceSchema deviceSchema : schemaList) {
            TimeseriesSchema timeseriesSchema = createTimeseries(deviceSchema);
            timeseriesSchemas.add(timeseriesSchema);
        }
        return timeseriesSchemas;
    }

    private void registerTimeseries(Session metaSession, List<TimeseriesSchema> timeseriesSchemas)
            throws TsdbException {
        // create time series
        for (TimeseriesSchema timeseriesSchema : timeseriesSchemas) {
            try {
                if (config.isVECTOR()) {
                    metaSession.createAlignedTimeseries(
                            timeseriesSchema.getDeviceId(),
                            timeseriesSchema.getPaths(),
                            timeseriesSchema.getTsDataTypes(),
                            timeseriesSchema.getTsEncodings(),
                            timeseriesSchema.getCompressionTypes(),
                            null);
                } else {
                    metaSession.createMultiTimeseries(
                            timeseriesSchema.getPaths(),
                            timeseriesSchema.getTsDataTypes(),
                            timeseriesSchema.getTsEncodings(),
                            timeseriesSchema.getCompressionTypes(),
                            null,
                            null,
                            null,
                            null);
                }
            } catch (Exception e) {
                handleRegisterException(e);
            }
        }
    }
    String getEncodingType(SensorType dataSensorType) {
        switch (dataSensorType) {
            case BOOLEAN:
                return config.getENCODING_BOOLEAN();
            case INT32:
                return config.getENCODING_INT32();
            case INT64:
                return config.getENCODING_INT64();
            case FLOAT:
                return config.getENCODING_FLOAT();
            case DOUBLE:
                return config.getENCODING_DOUBLE();
            case TEXT:
                return config.getENCODING_TEXT();
            default:
                LOGGER.error("Unsupported data sensorType {}.", dataSensorType);
                return null;
        }
    }
}
