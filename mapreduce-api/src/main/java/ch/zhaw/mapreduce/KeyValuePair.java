package ch.zhaw.mapreduce;

import java.io.Serializable;

/**
 * Ein Paar aus einem Schöüssel und einem Wert.
 * 
 * @author Max
 * 
 */
public final class KeyValuePair<K, V> implements Serializable {

	private static final long serialVersionUID = 1312114797122004977L;

	private final K key;

	private final V value;

	public KeyValuePair(K key, V value) {
		if (key == null || value == null) {
			throw new IllegalArgumentException("Neither argument must be null");
		}
		this.key = key;
		this.value = value;
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		return key.hashCode() * 31 + value.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof KeyValuePair)) {
			return false;
		}
		@SuppressWarnings("rawtypes")
		KeyValuePair other = (KeyValuePair) obj;
		return key.equals(other.key) && value.equals(other.value);
	}

	@Override
	public String toString() {
		return key.toString() + " = " + value.toString();
	}
}
