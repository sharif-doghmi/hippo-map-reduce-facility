package worker;

/*
 * Object that stores a key-value pair of generic types
 */
public class KeyValuePair<K,V> {
	
	private K key;
    private V value;
    
    public K getKey() {
		return key;
	}
    public V getValue() {
		return value;
	}
	public KeyValuePair(K key, V value){
        this.key = key;
        this.value = value;
    }
}