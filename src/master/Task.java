package master;
import messages.MasterMessage;
public class Task {
	String targetHost;
	MasterMessage mm;
	
	public Task(String host, MasterMessage mm) {
		targetHost = host;
		this.mm = mm;
	}
}
