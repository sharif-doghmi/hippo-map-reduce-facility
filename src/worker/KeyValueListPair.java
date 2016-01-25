package worker;
import java.util.ArrayList;

/*
 *  * Object that stores a <key, value-list> pair where key and values are of generic types
 */
public class KeyValueListPair<K, V> {
	
	private K key;
    private ArrayList<V> valueList;
    
	public KeyValueListPair(K key){
        this.key = key;
        this.valueList = new ArrayList<V>();
    }
	public K getKey() {
		return key;
	}
    public ArrayList<V> getValueList() {
		return valueList;
	}
    public void addValue(V value) {
    	valueList.add(value);
    }
}
