package cn.edu.tsinghua.iotdb.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;

public class CommandCli {
	private final String HELP_ARGS = "help";
	
	private final String CONFIG_ARGS = "cf";
	private final String CONFIG_NAME = "config file";
	
//	private final String HOST_ARGS = "h";
//	private final String HOST_NAME = "host";
	
//	private final String PORT_ARGS = "p";
//	private final String PORT_NAME = "port";
//	
//	private final String MODE_ARGS = "m";
//	private final String MODE_NAME = "mode";
//	
//	private final String DEVICE_ARGS = "dn";
//	private final String DEVICE_NAME = "device";
//	private final String SENSOR_ARGS = "sn";
//	private final String SENSOR_NAME = "sensor";
	
	private static final int MAX_HELP_CONSOLE_WIDTH = 88;
	
	private Options createOptions() {
		Options options = new Options();
		Option help = new Option(HELP_ARGS, false, "Display help information");
		help.setRequired(false);
		options.addOption(help);

//		Option host = Option.builder(HOST_ARGS).argName(HOST_NAME).hasArg().desc("Host Name (required)").build();
//		options.addOption(host);
//
//		Option port = Option.builder(PORT_ARGS).argName(PORT_NAME).hasArg().desc("Port (required)").build();
//		options.addOption(port);
//		
//		Option mode = Option.builder(MODE_ARGS).argName(MODE_NAME).hasArg().desc("Mode (required)").build();
//		options.addOption(mode);
//		
//		Option device = Option.builder(DEVICE_ARGS).argName(DEVICE_NAME).hasArg().desc("Device number (optional)").build();
//		options.addOption(device);
//		
//		Option sensor = Option.builder(SENSOR_ARGS).argName(SENSOR_NAME).hasArg().desc("Sensor number (optional)").build();
//		options.addOption(sensor);
		
		Option config = Option.builder(CONFIG_ARGS).argName(CONFIG_NAME).hasArg().desc("Config file path (optional)").build();
		options.addOption(config);
		
		return options;
	}
	
	private boolean parseParams(CommandLine commandLine, CommandLineParser parser, Options options, String[] args, HelpFormatter hf){
		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption(HELP_ARGS)) {
				hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
				return false;
			}

			if(commandLine.hasOption(CONFIG_ARGS)) {
				System.setProperty(Constants.BENCHMARK_CONF, commandLine.getOptionValue(CONFIG_ARGS));
				System.out.println(commandLine.getOptionValue(CONFIG_ARGS));
			}
			
		} catch (ParseException e) {
			System.out.println("Require more params input, please check the following hint.");
			hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
			return false;
		} catch (Exception e) {
			System.out.println("Error params input, because "+e.getMessage());
			hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
			return false;
		}
		return true;
	}
	
	public boolean init(String[] args){
		Options options = createOptions();
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
		CommandLine commandLine = null;
		CommandLineParser parser = new DefaultParser();

		if (args == null || args.length == 0) {
			System.out.println("Require more params input, please check the following hint.");
			hf.printHelp(Constants.CONSOLE_PREFIX, options, true);
			return false;
		}
		return parseParams(commandLine, parser, options, args, hf);
	}
	
	public static void main(String[] args) {

	}

}
