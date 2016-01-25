package common;
import java.io.BufferedReader;
import java.io.FileReader;


public class ConfigReader {
	public String masterHost;
	public String[] slaveHosts;
	public int[] slaveCores;
	public int masterListenPort;
	public int slaveListenPort;
	public int hbPort;
	public int hbInterval;

	public ConfigReader(String configFile) {
		try {
			BufferedReader r = new BufferedReader(new FileReader(configFile));
			String input;
			String[] split;
			int numSlaves;
			
			input = r.readLine();
			split = input.split("=");
			masterHost = split[1];
			
			input = r.readLine();
			split = input.split("=");
			masterListenPort = Integer.parseInt(split[1]);
			
			input = r.readLine();
			split = input.split("=");
			slaveListenPort = Integer.parseInt(split[1]);
			
			input = r.readLine();
			split = input.split("=");
			hbPort = Integer.parseInt(split[1]);
			
			input = r.readLine();
			split = input.split("=");
			hbInterval = Integer.parseInt(split[1]);
			
			input = r.readLine();
			split = input.split("=");
			numSlaves = Integer.parseInt(split[1]);
			
			slaveHosts = new String[numSlaves];
			slaveCores = new int[numSlaves];
			input = r.readLine();
			
			for(int i=0; i<numSlaves; i++) {
				input = r.readLine();
				split = input.split("\\s");
				slaveHosts[i] = split[0];
				slaveCores[i] = Integer.parseInt(split[1]);
			}
			
			r.close();
			
		} catch(Exception e) {
			System.out.println("Failed to read config file.");
		}
	}
	
}
