package master;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

//launches threads to deal with each heartbeat connection it receives
public class HBListener implements Runnable {

	int hbport;
	int hbinterval;
	ServerSocket ss;
	ConcurrentHashMap<String,NodeInfo> nodeData;
	
	
	public HBListener(int hbport, int hbinterval, 
			ConcurrentHashMap<String,NodeInfo> data) {
		this.hbport = hbport;
		this.hbinterval = hbinterval;
		this.nodeData = data;
		try {
			ss = new ServerSocket(hbport);
			ss.setSoTimeout(10000);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public void run() {
		Socket s;
		Thread t;

		while(true){
			try {
				s = ss.accept();				
				t = new Thread(new HBHandler(s, hbinterval, nodeData));
				t.start();
			} catch (IOException e) {
				//socket timed out
			}
		}
	}
}
