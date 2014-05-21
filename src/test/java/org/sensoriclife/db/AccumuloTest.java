package org.sensoriclife.db;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sensoriclife.util.Helpers;

/**
 *
 * @author jnphilipp
 * @version 0.0.3
 */
public class AccumuloTest {
	@Rule
	public TemporaryFolder tmpDirectory = new TemporaryFolder();

	@Test
	@Deprecated
	public void testAccumulo() throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException, MutationsRejectedException, TableExistsException, TableNotFoundException {
		Accumulo accumulo = Accumulo.getInstance();
		accumulo.connect();
		accumulo.createTable("electricity_consumption");

		Value value = new Value("0".getBytes());
		accumulo.write("electricity_consumption", "1", "electricity", "", value);

		value = new Value("1".getBytes());
		accumulo.write("electricity_consumption", "2", "electricity", "", value);

		value = new Value("5".getBytes());
		accumulo.write("electricity_consumption", "3", "electricity", "", value);

		value = new Value("5".getBytes());
		accumulo.write("electricity_consumption", "4", "electricity", "", value);

		Iterator<Entry<Key, Value>> entries = accumulo.scanByKey("electricity_consumption", "public", new Range("2", "3"));
		int i = 0;
		while ( entries.hasNext() ) {
			Entry<Key, Value> entry = entries.next();
			i++;
		}

		assertEquals(2, i);
		accumulo.deleteTable("electricity_consumption");
		accumulo.disconnect();
	}

	@Test
	@Deprecated
	public void testAccumuloVersionLimit() throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException, MutationsRejectedException, TableExistsException, TableNotFoundException {
		Accumulo accumulo = Accumulo.getInstance();
		accumulo.connect();
		accumulo.createTable("electricity_consumption", false);

		Value value = new Value("0".getBytes());
		accumulo.write("electricity_consumption", "1", "electricity", "", 1, value);

		value = new Value("1".getBytes());
		accumulo.write("electricity_consumption", "1", "electricity", "", 2, value);

		value = new Value("5".getBytes());
		accumulo.write("electricity_consumption", "1", "electricity", "", 3, value);

		value = new Value("5".getBytes());
		accumulo.write("electricity_consumption", "1", "electricity", "", 4, value);

		Iterator<Entry<Key, Value>> entries = accumulo.scanAll("electricity_consumption", "public");
		int i = 0;
		while ( entries.hasNext() ) {
			Entry<Key, Value> entry = entries.next();
			i++;
		}

		assertEquals(4, i);
		accumulo.deleteTable("electricity_consumption");
		accumulo.disconnect();
	}

	@Test
	public void testMiniCluster() throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException, MutationsRejectedException, TableExistsException, TableNotFoundException {
		Accumulo accumulo = Accumulo.getInstance();
		accumulo.connect(this.tmpDirectory.newFolder(), "password");
		accumulo.createTable("electricity_consumption", false);

		Value value = new Value("0".getBytes());
		accumulo.addMutation("electricity_consumption", "1", "electricity", "", 1, value);

		value = new Value("1".getBytes());
		accumulo.addMutation("electricity_consumption", "1", "electricity", "", 2, value);

		value = new Value("5".getBytes());
		accumulo.addMutation("electricity_consumption", "1", "electricity", "", 3, value);

		value = new Value("5".getBytes());
		accumulo.addMutation("electricity_consumption", "1", "electricity", "", 4, value);

		accumulo.addMutation("electricity_consumption", "7", "electricity", "", 5, "6".getBytes());
		accumulo.addMutation("electricity_consumption", "8", "electricity", "", 5, "9".getBytes());
		accumulo.closeBashWriter("electricity_consumption");

		Iterator<Entry<Key, Value>> entries = accumulo.scanAll("electricity_consumption");
		int i = 0;
		while ( entries.hasNext() ) {
			Entry<Key, Value> entry = entries.next();
			i++;
		}
		assertEquals(6, i);

		accumulo.deleteTable("electricity_consumption");
		accumulo.disconnect();
	}

	@Test
	public void testAddMutation() throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException, MutationsRejectedException, TableExistsException, TableNotFoundException {
		Accumulo accumulo = Accumulo.getInstance();
		accumulo.connect();
		accumulo.createTable("test_batch");

		accumulo.addMutation("test_batch", "1".getBytes(), "fam".getBytes(), "".getBytes(), "5".getBytes());
		accumulo.addMutation("test_batch", "2".getBytes(), "fam".getBytes(), "".getBytes(), 5, "3".getBytes());
		accumulo.addMutation("test_batch", "3".getBytes(), "fam".getBytes(), "".getBytes(), 8, "8".getBytes());
		accumulo.addMutation("test_batch", "4".getBytes(), "fam".getBytes(), "".getBytes(), "0".getBytes());
		accumulo.flushBashWriter("test_batch");

		Iterator<Entry<Key, Value>> entries = accumulo.scanAll("test_batch");
		int i = 0;
		while ( entries.hasNext() ) {
			Entry<Key, Value> entry = entries.next();
			i++;
		}
		assertEquals(4, i);

		accumulo.addMutation("test_batch", "5", "fam", "", Helpers.toByteArray(5.0f));
		accumulo.addMutation("test_batch", "6", "fam", "", Helpers.toByteArray(13.5f));
		accumulo.closeBashWriter("test_batch");

		entries = accumulo.scanAll("test_batch");
		i = 0;
		while ( entries.hasNext() ) {
			Entry<Key, Value> entry = entries.next();
			i++;
		}
		assertEquals(6, i);

		accumulo.deleteTable("test_batch");
		accumulo.disconnect();
	}
}