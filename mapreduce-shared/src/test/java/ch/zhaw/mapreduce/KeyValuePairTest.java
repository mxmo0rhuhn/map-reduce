package ch.zhaw.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


public class KeyValuePairTest {
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldNotAcceptNullKeys() {
		new KeyValuePair<String, String>(null, "value");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldNotAcceptNullValues() {
		new KeyValuePair<String, String>("key", null);
	}
	
	@Test
	public void shouldSetKey() {
		KeyValuePair<String, String> kvp = new KeyValuePair<String, String>("key", "value");
		assertEquals("key", kvp.getKey());
	}

	@Test
	public void shouldSetValue() {
		KeyValuePair<String, String> kvp = new KeyValuePair<String, String>("key", "value");
		assertEquals("value", kvp.getValue());
	}

	@Test
	public void differentKeyShouldGiveDifferentHashCode() {
		KeyValuePair<String, String> kvp1 = new KeyValuePair<String, String>("key", "value");
		KeyValuePair<String, String> kvp2 = new KeyValuePair<String, String>("key2", "value");
		assertFalse(kvp1.hashCode() == kvp2.hashCode());
	}
	
	@Test
	public void differentKeyShouldNotBeEqual() {
		KeyValuePair<String, String> kvp1 = new KeyValuePair<String, String>("key", "value");
		KeyValuePair<String, String> kvp2 = new KeyValuePair<String, String>("key2", "value");
		assertFalse(kvp1.equals(kvp2));
	}
	
	@Test
	public void differentValueShouldGiveDifferentHashCode() {
		KeyValuePair<String, String> kvp1 = new KeyValuePair<String, String>("key", "value");
		KeyValuePair<String, String> kvp2 = new KeyValuePair<String, String>("key", "value2");
		assertFalse(kvp1.hashCode() == kvp2.hashCode());
	}
	
	@Test
	public void differentValueShouldNotBeEqual() {
		KeyValuePair<String, String> kvp1 = new KeyValuePair<String, String>("key", "value");
		KeyValuePair<String, String> kvp2 = new KeyValuePair<String, String>("key", "value2");
		assertFalse(kvp1.equals(kvp2));
	}
	
	@Test
	public void shouldBeEqualWithSameKeyAndValue() {
		KeyValuePair<String, String> kvp1 = new KeyValuePair<String, String>("key", "value");
		KeyValuePair<String, String> kvp2 = new KeyValuePair<String, String>("key", "value");
		assertEquals(kvp1,kvp2);
	}
	
	@Test
	public void shouldHaveSameHashCodeWithSameKeyAndValue() {
		KeyValuePair<String, String> kvp1 = new KeyValuePair<String, String>("key", "value");
		KeyValuePair<String, String> kvp2 = new KeyValuePair<String, String>("key", "value");
		assertTrue(kvp1.hashCode() == kvp2.hashCode());
	}
	
	@Test
	public void shouldNotBeEqualsToOtherType() {
		KeyValuePair<String, String> kvp = new KeyValuePair<String, String>("key", "value");
		Object o = new Object();
		assertFalse(kvp.equals(o));
	}
	
	@Test
	public void shouldShowKeyAndValue() {
		KeyValuePair<String, String> kvp = new KeyValuePair<String, String>("key", "value");
		assertEquals("key = value", kvp.toString());
	}
}
