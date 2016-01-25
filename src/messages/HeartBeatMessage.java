package messages;
import java.io.Serializable;

//a simple message sent with each heartbeat
//content is unimportant since it is the receiving of the message that counts
public class HeartBeatMessage implements Serializable {

	String heartBeat;
	private static final long serialVersionUID = 1L;
	
	public HeartBeatMessage() {
		this.heartBeat = "I'm alive!";
	}
}