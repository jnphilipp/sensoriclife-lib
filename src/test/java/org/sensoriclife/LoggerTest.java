package org.sensoriclife;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 *
 * @author jnphilipp
 * @version 0.0.1
 */
public class LoggerTest {
	/**
	 * Test of debug method, of class Logger.
	 */
	@Test
	public void testDebug() {
		Logger.debug("test");
		Logger.debug("test1", "test2");
		Logger.debug(LoggerTest.class, "test");
		Logger.debug(LoggerTest.class, "test1", "test2");
		assertTrue(true);
	}

	/**
	 * Test of error method, of class Logger.
	 */
	@Test
	public void testError() {
		Logger.error("test");
		Logger.error("test1", "test2");
		Logger.error(LoggerTest.class, "test");
		Logger.error(LoggerTest.class, "test1", "test2");
		assertTrue(true);
	}

	/**
	 * Test of info method, of class Logger.
	 */
	@Test
	public void testInfo() {
		Logger.info("test");
		Logger.info("test1", "test2");
		Logger.info(LoggerTest.class, "test");
		Logger.info(LoggerTest.class, "test1", "test2");
		assertTrue(true);
	}

	/**
	 * Test of warn method, of class Logger.
	 */
	@Test
	public void testWarn() {
		Logger.warn("test");
		Logger.warn("test1", "test2");
		Logger.warn(LoggerTest.class, "test");
		Logger.warn(LoggerTest.class, "test1", "test2");
		assertTrue(true);
	}
}