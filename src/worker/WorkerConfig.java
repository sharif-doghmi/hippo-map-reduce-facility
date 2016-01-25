package worker;
import common.*;

/*
 * WorkerConfig object that contains static variables storing configuration data initialized from config file.
 * Can only be initialized once at run-time by main thread.
 */
public class WorkerConfig {

	private static String masterHostName;
	private static int masterPort;
	private static int masterHBPort;
	private static int listenPort;
	private static int heartBeatPeriod;
	
	// flag to indicate that this object has been initialized. To defeat attempts to reinitialize
	private static boolean initialized = false;
	
	public WorkerConfig(String configFile) {
		if (initialized == false) {		// if not initialized yet, initialize
			initialized = true;
			ConfigReader cr = new ConfigReader(configFile);		// Config reader object parses config file
			
			/*
			 * Populate static variables of WorkerConfig class using config reader data
			 */
			WorkerConfig.masterHostName = cr.masterHost;
			WorkerConfig.masterPort = cr.masterListenPort;
			WorkerConfig.masterHBPort = cr.hbPort; 
			WorkerConfig.listenPort = cr.slaveListenPort;
			WorkerConfig.heartBeatPeriod = cr.hbInterval;
		}
	}

	public static String getMasterHostName() {
		return masterHostName;
	}

	public static int getMasterPort() {
		return masterPort;
	}

	public static int getMasterHBPort() {
		return masterHBPort;
	}
	
	public static int getListenPort() {
		return listenPort;
	}

	public static int getHeartBeatPeriod() {
		return heartBeatPeriod;
	}
}
