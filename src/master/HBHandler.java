package master;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;

//handles the heartbeat messages
public class HBHandler implements Runnable {

	int timeout;
	Socket s;
	ConcurrentHashMap<String, NodeInfo> nodeData;
	
	public HBHandler(Socket s, int interval, 
			ConcurrentHashMap<String, NodeInfo> nodeData) throws SocketException {
		this.s = s;
		this.timeout = interval;
		this.s.setSoTimeout(timeout);
		this.nodeData = nodeData;
	}
	//when we receive a heartbeat, record the current time
	public void run() {
		try {
			InputStream is = s.getInputStream();
			ObjectInputStream oi = new ObjectInputStream(is);
			InetSocketAddress remoteAddr = (InetSocketAddress) s.getRemoteSocketAddress();
			NodeInfo ni;


			oi.readObject();
			oi.close();
			s.close();
			
			ni = nodeData.get(remoteAddr.getHostName().toLowerCase());

			ni.lock.lock();
			try {
				ni.lastHB = System.currentTimeMillis();
			} finally {
				ni.lock.unlock();
			}



		}
		catch(Exception e) {
			//Socket TIMEOUT
		}
	}

}
