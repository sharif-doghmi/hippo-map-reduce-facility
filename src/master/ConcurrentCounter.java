package master;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class ConcurrentCounter {
	Lock lock;
	int counter;
	
	public ConcurrentCounter() {
		counter = 0;
		lock = new ReentrantLock();
	}
}
