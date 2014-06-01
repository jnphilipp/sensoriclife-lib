package org.sensoriclife;

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class ConfigTest {
	@Test
	public void testToMap() {
		Config.getInstance();
		Config.getInstance().getProperties().setProperty("test", "blub");
		Config.getInstance().getProperties().setProperty("hallo", "55");
		Config.getInstance().getProperties().setProperty("cold", "true");

		Map<String, String> defaults = new LinkedHashMap<>();
		defaults.put("some", "5");
		defaults.put("thing", "sun");
		defaults.put("cold", "false");
		Config.getInstance().setDefaults(defaults);

		Map<String, String> expResult = new LinkedHashMap<>();
		expResult.put("test", "blub");
		expResult.put("hallo", "55");
		expResult.put("some", "5");
		expResult.put("thing", "sun");
		expResult.put("cold", "true");

		Map<String, String> result = Config.toMap();
		assertEquals(expResult, result);
	}
}