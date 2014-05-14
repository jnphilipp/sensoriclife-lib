package org.sensoriclife;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class Config {
	/**
	 * instance
	 */
	private static Config instance;
	/**
	 * properties
	 */
	private Properties properties;
	/**
	 * 
	 */
	private Map<String, String> defaults;

	private Config() {
		try {
			this.properties = new Properties();
			this.properties.load(this.getClass().getResourceAsStream("/config.properties"));
		}
		catch ( IOException e ) {
			Logger.error("Error while loading config file.", e.toString());
			System.exit(1);
		}

		this.defaults = new LinkedHashMap<>();
	}

	public static Config getInstance() {
		if ( instance == null )
			instance = new Config();

		return instance;
	}

	/**
	 * @return the properties
	 */
	public Properties getProperties() {
		return this.properties;
	}

	/**
	 * @return the defaults
	 */
	public Map<String, String> getDefaults() {
		return defaults;
	}

	/**
	 * @param defaults the defaults to set
	 */
	public void setDefaults(Map<String, String> defaults) {
		this.defaults = defaults;
	}

	/**
	 * @param key key
	 * @return values of the given key
	 */
	public static String getProperty(String key) {
		if ( instance.getDefaults().containsKey(key) )
			return instance.getDefaults().get(key);
		else
			return instance.getProperties().getProperty(key, "");
	}

	/**
	 * Returns the value of the given key as boolean.
	 * @param key key
	 * @return <code>true</code> or <code>false</code>
	 */
	public static boolean getBooleanProperty(String key) {
		return Boolean.valueOf(Config.getProperty(key));
	}

	/**
	 * Returns the value of the given key as integer.
	 * @param key key
	 * @return integer value
	 */
	public static int getIntegerProperty(String key) {
		return Integer.parseInt(Config.getProperty(key));
	}
}