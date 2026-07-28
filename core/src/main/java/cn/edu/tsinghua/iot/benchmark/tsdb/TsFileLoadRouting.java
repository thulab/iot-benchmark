package cn.edu.tsinghua.iot.benchmark.tsdb;

import cn.edu.tsinghua.iot.benchmark.conf.Config;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps SCP staging and LOAD Session on the same DataNode for one benchmark client.
 *
 * <p>{@code HOST/PORT}, {@code TSFILE_LOAD_REMOTE_HOST}, and optional passwords are parallel lists
 * in the same cluster order. Client {@code n} always uses index {@code n % HOST.size()}.
 */
public final class TsFileLoadRouting {
  private TsFileLoadRouting() {}

  public static void validate(Config config, DBConfig dbConfig) {
    List<String> nodes = dbConfig.getHOST();
    List<String> ports = dbConfig.getPORT();
    if (nodes == null || nodes.isEmpty()) {
      throw new IllegalArgumentException("HOST must not be empty for tsFileLoadMode");
    }
    if (ports == null || ports.size() != nodes.size()) {
      throw new IllegalArgumentException(
          "HOST and PORT must have the same length for tsFileLoadMode");
    }
    for (int i = 0; i < nodes.size(); i++) {
      if (nodes.get(i) == null || nodes.get(i).trim().isEmpty()) {
        throw new IllegalArgumentException("HOST[" + i + "] must not be empty");
      }
      if (ports.get(i) == null || ports.get(i).trim().isEmpty()) {
        throw new IllegalArgumentException("PORT[" + i + "] must not be empty");
      }
    }

    String remoteHostsRaw = config.getTSFILE_LOAD_REMOTE_HOST().trim();
    if (remoteHostsRaw.isEmpty()) {
      return;
    }
    List<String> remoteHosts = splitCsv(remoteHostsRaw);
    if (remoteHosts.size() != nodes.size()) {
      throw new IllegalArgumentException(
          "TSFILE_LOAD_REMOTE_HOST must have the same length and order as HOST so each client "
              + "SCPs to the DataNode that executes its LOAD. HOST size="
              + nodes.size()
              + ", TSFILE_LOAD_REMOTE_HOST size="
              + remoteHosts.size());
    }
    for (int i = 0; i < nodes.size(); i++) {
      String remoteHost = remoteHosts.get(i);
      if (remoteHost.isEmpty()) {
        throw new IllegalArgumentException("TSFILE_LOAD_REMOTE_HOST[" + i + "] must not be empty");
      }
      String remoteAddress = sshAddress(remoteHost);
      String loadAddress = nodes.get(i).trim();
      if (!sameHost(remoteAddress, loadAddress)) {
        throw new IllegalArgumentException(
            "TSFILE_LOAD_REMOTE_HOST["
                + i
                + "]="
                + remoteHost
                + " does not match HOST["
                + i
                + "]="
                + loadAddress
                + ". Keep HOST and TSFILE_LOAD_REMOTE_HOST in the same cluster order.");
      }
    }

    String passwordsRaw = config.getTSFILE_LOAD_REMOTE_PASSWORD();
    if (passwordsRaw != null && !passwordsRaw.trim().isEmpty()) {
      List<String> passwords = splitCsvKeepEmpty(passwordsRaw);
      if (passwords.size() != 1 && passwords.size() != remoteHosts.size()) {
        throw new IllegalArgumentException(
            "TSFILE_LOAD_REMOTE_PASSWORD must be empty (SSH key), one shared password, or one "
                + "password per TSFILE_LOAD_REMOTE_HOST entry. remote hosts="
                + remoteHosts.size()
                + ", passwords="
                + passwords.size());
      }
    }

    if (config.getTSFILE_LOAD_REMOTE_DIR().trim().isEmpty()) {
      throw new IllegalArgumentException(
          "TSFILE_LOAD_REMOTE_DIR is required when TSFILE_LOAD_REMOTE_HOST is set");
    }
  }

  public static int nodeIndex(int clientId, int nodeCount) {
    if (nodeCount <= 0) {
      throw new IllegalArgumentException("nodeCount must be greater than 0");
    }
    return Math.floorMod(clientId, nodeCount);
  }

  public static String loadEndpoint(DBConfig dbConfig, int clientId) {
    int index = nodeIndex(clientId, dbConfig.getHOST().size());
    return dbConfig.getHOST().get(index).trim() + ":" + dbConfig.getPORT().get(index).trim();
  }

  public static RemoteTarget remoteTarget(Config config, DBConfig dbConfig, int clientId) {
    validate(config, dbConfig);
    if (config.getTSFILE_LOAD_REMOTE_HOST().trim().isEmpty()) {
      return null;
    }
    int index = nodeIndex(clientId, dbConfig.getHOST().size());
    List<String> remoteHosts = splitCsv(config.getTSFILE_LOAD_REMOTE_HOST().trim());
    String sshHost = remoteHosts.get(index);
    String password =
        selectPassword(config.getTSFILE_LOAD_REMOTE_PASSWORD(), index, remoteHosts.size());
    return new RemoteTarget(sshHost, password, index);
  }

  private static String selectPassword(String passwords, int nodeIndex, int remoteHostCount) {
    if (passwords == null || passwords.trim().isEmpty()) {
      return "";
    }
    List<String> values = splitCsvKeepEmpty(passwords);
    if (values.size() == 1) {
      return values.get(0);
    }
    if (values.size() != remoteHostCount) {
      throw new IllegalArgumentException(
          "TSFILE_LOAD_REMOTE_PASSWORD size must be 1 or equal to TSFILE_LOAD_REMOTE_HOST size");
    }
    return values.get(nodeIndex);
  }

  private static List<String> splitCsv(String raw) {
    String[] parts = raw.split(",");
    List<String> values = new ArrayList<>(parts.length);
    for (String part : parts) {
      values.add(part.trim());
    }
    return values;
  }

  private static List<String> splitCsvKeepEmpty(String raw) {
    String[] parts = raw.split(",", -1);
    List<String> values = new ArrayList<>(parts.length);
    for (String part : parts) {
      values.add(part.trim());
    }
    return values;
  }

  /** Extracts the host/IP from {@code user@host} or bare {@code host}. */
  static String sshAddress(String sshTarget) {
    int at = sshTarget.lastIndexOf('@');
    return at >= 0 ? sshTarget.substring(at + 1).trim() : sshTarget.trim();
  }

  private static boolean sameHost(String left, String right) {
    return normalizeHost(left).equalsIgnoreCase(normalizeHost(right));
  }

  private static String normalizeHost(String host) {
    String value = host.trim();
    if (value.startsWith("[") && value.endsWith("]")) {
      value = value.substring(1, value.length() - 1);
    }
    return value;
  }

  public static final class RemoteTarget {
    private final String sshHost;
    private final String password;
    private final int nodeIndex;

    private RemoteTarget(String sshHost, String password, int nodeIndex) {
      this.sshHost = sshHost;
      this.password = password;
      this.nodeIndex = nodeIndex;
    }

    public String getSshHost() {
      return sshHost;
    }

    public String getPassword() {
      return password;
    }

    public int getNodeIndex() {
      return nodeIndex;
    }
  }
}
