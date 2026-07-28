package cn.edu.tsinghua.iot.benchmark.tsdb;

import cn.edu.tsinghua.iot.benchmark.conf.Config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Uses the local SSH configuration to stage a TsFile where the DataNode can load it. */
public final class TsFileLoadTransfer {
  private static final Map<String, Boolean> PREPARED_REMOTE_DIRS = new ConcurrentHashMap<>();
  private static final Map<String, File> ASKPASS_FILES = new ConcurrentHashMap<>();
  private static final String CONTROL_DIR =
      System.getProperty("java.io.tmpdir") + "/iot-benchmark-ssh-control";
  private static final String KNOWN_HOSTS_FILE = CONTROL_DIR + "/known_hosts";

  private TsFileLoadTransfer() {}

  /**
   * Stages {@code local} onto the DataNode assigned to {@code clientId}. The SCP target is chosen
   * with the same index as the client's fixed LOAD Session ({@code clientId % HOST.size()}).
   */
  public static StagedFile stage(File local, Config config, DBConfig dbConfig, int clientId)
      throws Exception {
    TsFileLoadRouting.RemoteTarget remote =
        TsFileLoadRouting.remoteTarget(config, dbConfig, clientId);
    if (remote == null) {
      return new StagedFile(local.getCanonicalPath(), null, "", 0);
    }
    String remoteDir = config.getTSFILE_LOAD_REMOTE_DIR().replaceAll("/+$", "");
    String remotePath = remoteDir + "/" + local.getName();
    long start = System.nanoTime();
    try {
      ensureRemoteDirectory(remote.getSshHost(), remoteDir, remote.getPassword());
      run(
          remote.getPassword(),
          scpCommand(
              local.getCanonicalPath(),
              remote.getSshHost() + ":" + remotePath,
              remote.getSshHost()));
      return new StagedFile(
          remotePath, remote.getSshHost(), remote.getPassword(), System.nanoTime() - start);
    } catch (Exception e) {
      if (config.isTSFILE_LOAD_CLEANUP()) Files.deleteIfExists(local.toPath());
      throw e;
    }
  }

  public static void cleanup(StagedFile staged, File local, Config config) throws Exception {
    Exception failure = null;
    if (config.isTSFILE_LOAD_CLEANUP() && staged.host != null) {
      try {
        run(staged.password, sshCommand(staged.host, "rm -f -- " + quote(staged.path)));
      } catch (Exception e) {
        failure = e;
      }
    }
    if (config.isTSFILE_LOAD_CLEANUP()) Files.deleteIfExists(local.toPath());
    if (failure != null) throw failure;
  }

  private static void ensureRemoteDirectory(String host, String remoteDir, String password)
      throws Exception {
    String key = host + "|" + remoteDir;
    if (PREPARED_REMOTE_DIRS.putIfAbsent(key, Boolean.TRUE) != null) {
      return;
    }
    try {
      run(password, sshCommand(host, "mkdir -p -- " + quote(remoteDir)));
    } catch (Exception e) {
      PREPARED_REMOTE_DIRS.remove(key);
      throw e;
    }
  }

  private static String[] sshCommand(String host, String remoteCommand) throws IOException {
    List<String> command = new ArrayList<>();
    command.add("ssh");
    addControlMasterOptions(command, host);
    command.add(host);
    command.add(remoteCommand);
    return command.toArray(new String[0]);
  }

  private static String[] scpCommand(String localPath, String remoteSpec, String host)
      throws IOException {
    List<String> command = new ArrayList<>();
    command.add("scp");
    addControlMasterOptions(command, host);
    command.add(localPath);
    command.add(remoteSpec);
    return command.toArray(new String[0]);
  }

  private static void addControlMasterOptions(List<String> command, String host)
      throws IOException {
    Files.createDirectories(new File(CONTROL_DIR).toPath());
    String controlPath = CONTROL_DIR + "/cm-" + host.replaceAll("[^A-Za-z0-9._@-]", "_") + "-%p";
    command.add("-o");
    command.add("ControlMaster=auto");
    command.add("-o");
    command.add("ControlPath=" + controlPath);
    command.add("-o");
    command.add("ControlPersist=300");
    // Password-mode SSH is non-interactive because it uses SSH_ASKPASS. Without accept-new, the
    // first connection to a DataNode blocks on the host-key confirmation prompt and every client
    // routed to that node stalls. accept-new records only an unknown key; a changed known key is
    // still rejected, unlike StrictHostKeyChecking=no.
    command.add("-o");
    command.add("StrictHostKeyChecking=accept-new");
    command.add("-o");
    command.add("UserKnownHostsFile=" + KNOWN_HOSTS_FILE);
  }

  private static void run(String password, String... command)
      throws IOException, InterruptedException {
    if (password.isEmpty()) {
      runCommand(command, null);
      return;
    }
    File askPass = ASKPASS_FILES.computeIfAbsent(password, TsFileLoadTransfer::createAskPass);
    runCommand(command, askPass, password);
  }

  private static File createAskPass(String password) {
    try {
      File askPass = Files.createTempFile("iot-benchmark-ssh-askpass-", ".sh").toFile();
      askPass.deleteOnExit();
      Files.write(
          askPass.toPath(),
          Arrays.asList("#!/bin/sh", "printf '%s\\n' \"$TSFILE_LOAD_REMOTE_PASSWORD\""));
      if (!askPass.setExecutable(true, true)) {
        throw new IllegalStateException("Unable to make SSH password helper executable");
      }
      return askPass;
    } catch (IOException e) {
      throw new IllegalStateException("Unable to create SSH password helper", e);
    }
  }

  private static void runCommand(String[] command, File askPass)
      throws IOException, InterruptedException {
    runCommand(command, askPass, null);
  }

  private static void runCommand(String[] command, File askPass, String password)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder(command).inheritIO();
    if (askPass != null) {
      builder.environment().put("SSH_ASKPASS", askPass.getCanonicalPath());
      builder.environment().put("SSH_ASKPASS_REQUIRE", "force");
      builder.environment().put("DISPLAY", "iot-benchmark:0");
      builder.environment().put("TSFILE_LOAD_REMOTE_PASSWORD", password);
    }
    Process process = builder.start();
    if (process.waitFor() != 0) throw new IOException("Failed to run " + command[0]);
  }

  private static String quote(String value) {
    return "'" + value.replace("'", "'\\\"'\\\"'") + "'";
  }

  public static final class StagedFile {
    private final String path;
    private final String host;
    private final String password;
    private final long transferNanos;

    private StagedFile(String path, String host, String password, long transferNanos) {
      this.path = path;
      this.host = host;
      this.password = password;
      this.transferNanos = transferNanos;
    }

    public String getPath() {
      return path;
    }

    public long getTransferNanos() {
      return transferNanos;
    }
  }
}
