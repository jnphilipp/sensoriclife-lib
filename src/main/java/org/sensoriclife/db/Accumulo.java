package org.sensoriclife.db;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

/**
 *
 * @author jnphilipp
 * @version 0.1.0
 */
public class Accumulo {
	/**
	 * instance accumulo
	 */
	private static Accumulo accumolo;
	/**
	 * instance
	 */
	private Instance instance; 
	/**
	 * connector
	 */
	private Connector connector;

	private Accumulo() {
		this.instance = null;
		this.connector = null;
	}

	/**
	 * Returns an instance of this class.
	 * @return instance
	 */
	public static synchronized Accumulo getInstance() {
		if ( accumolo == null )
			accumolo = new Accumulo();

		return accumolo;
	}

	/**
	 * Returns a Scanner for the given table.
	 * @param table table
	 * @return scanner
	 * @throws TableNotFoundException
	 */
	public synchronized Scanner getScannder(String table) throws TableNotFoundException {
		return this.getScannder(table, "public");
	}

	/**
	 * Returns a Scanner for the given table.
	 * @param table table
	 * @param visibility column visibility
	 * @return scanner
	 * @throws TableNotFoundException
	 */
	public synchronized Scanner getScannder(String table, String visibility) throws TableNotFoundException {
		Authorizations auths = new Authorizations(visibility);
		return this.connector.createScanner(table, auths);
	}

	/**
	 * Connects to Accumulo using the MockIsntance.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	public synchronized void connect() throws AccumuloException, AccumuloSecurityException {
		this.instance = new MockInstance();
		this.connector = this.instance.getConnector("",  new PasswordToken(""));
	}

	/**
	 * Connects to an Accumulo cluster.
	 * @param name name
	 * @param zooServers zoo servers
	 * @param user user
	 * @param password password
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	public synchronized void connect(String name, String zooServers, String user, String password) throws AccumuloException, AccumuloSecurityException {
		this.instance = new ZooKeeperInstance(name, zooServers);
		this.connector = this.instance.getConnector(user, new PasswordToken(password));
	}

	/**
	 * Disconnects Accumulo.
	 */
	public synchronized void disconnect() {
		this.instance = null;
		this.connector = null;
	}

	/**
	 * Creates the given table.
	 * @param table table
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableExistsException
	 */
	public void createTable(String table) throws AccumuloException, AccumuloSecurityException, TableExistsException {
		this.connector.tableOperations().create(table);
	}

	/**
	 * Creates the given table.
	 * @param table table
	 * @param limitVersion <code>false</code> to store all timestamp versions
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableExistsException
	 */
	public void createTable(String table, boolean limitVersion) throws AccumuloException, AccumuloSecurityException, TableExistsException {
		this.connector.tableOperations().create(table, limitVersion);
	}

	/**
	 * Returns all elements of the given table.
	 * @param table table
	 * @return Iterator for all elements
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanAll(String table) throws TableNotFoundException {
		return this.scanAll(table, "public");
	}

	/**
	 * Returns all elements of the given table.
	 * @param table table
	 * @param visibility column visibility
	 * @return Iterator for all elements
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanAll(String table, String visibility) throws TableNotFoundException {
		Authorizations auths = new Authorizations(visibility);
		Scanner scanner = this.connector.createScanner(table, auths);

		Iterator<Entry<Key,Value>> iterator = scanner.iterator();
		scanner.close();

		return iterator;
	}

	/**
	 * Returns all elements filter by the given column family and column qualifier.
	 * @param table table
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @return iterator
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanColumns(String table, String columnFamily, String columnQualifier) throws TableNotFoundException {
		return this.scanColumns(table, columnFamily, columnQualifier, "public");
	}

	/**
	 * Returns all elements filter by the given column family and column qualifier.
	 * @param table table
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param visibility column visibility
	 * @return iterator
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanColumns(String table, String columnFamily, String columnQualifier, String visibility) throws TableNotFoundException {
		Authorizations auths = new Authorizations(visibility);

		Scanner scanner = this.connector.createScanner(table, auths);
		scanner.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
		Iterator<Entry<Key,Value>> iterator = scanner.iterator();
		scanner.close();

		return iterator;
	}

	/**
	 * Returns all elements filter by the given range for the row ids.
	 * @param table table
	 * @param range row id range
	 * @return iterator
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanByKey(String table, Range range) throws TableNotFoundException {
		return this.scanByKey(table, "public", range);
	}

	/**
	 * Returns all elements filter by the given range for the row ids.
	 * @param table table
	 * @param visibility column visibility
	 * @param range row id range
	 * @return iterator
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanByKey(String table, String visibility, Range range) throws TableNotFoundException {
		Authorizations auths = new Authorizations(visibility);

		Scanner scan = this.connector.createScanner(table, auths);
		scan.setRange(range);

		return scan.iterator();
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, "public", System.currentTimeMillis(), value);
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, String value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, "public", System.currentTimeMillis(), value);
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, Value value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, "public", System.currentTimeMillis(), value);
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param visibility column visibility
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, String visibility, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, visibility, System.currentTimeMillis(), value);
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param visibility column visibility
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, String visibility, String value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, visibility, System.currentTimeMillis(), value);
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param visibility column visibility
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, String visibility, Value value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, visibility, System.currentTimeMillis(), value);
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param timestamp timestamp
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, long timestamp, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, "public", timestamp, value);
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param timestamp timestamp
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, long timestamp, String value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, "public", timestamp, value);
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param timestamp timestamp
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, long timestamp, Value value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, "public", timestamp, value);
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param visibility column visibility
	 * @param timestamp timestamp
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, String visibility, long timestamp, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		ColumnVisibility colVis = new ColumnVisibility("public");

		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);
		BatchWriter writer = this.connector.createBatchWriter(table, config);

		Mutation mutation = new Mutation(rowId);
		mutation.put(columnFamily, columnQualifier, colVis, timestamp, value);
		writer.addMutation(mutation);

		writer.close();
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param visibility column visibility
	 * @param timestamp timestamp
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, String visibility, long timestamp, String value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, visibility, timestamp, new Value(value.getBytes()));
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param visibility column visibility
	 * @param timestamp timestamp
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, String visibility, long timestamp, Value value) throws MutationsRejectedException, TableNotFoundException {
		ColumnVisibility colVis = new ColumnVisibility("public");

		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);
		BatchWriter writer = this.connector.createBatchWriter(table, config);

		Mutation mutation = new Mutation(rowId);
		mutation.put(columnFamily, columnQualifier, colVis, timestamp, value);
		writer.addMutation(mutation);

		writer.close();
	}

	/**
	 * Writes the given data to the given table.
	 * @param table table
	 * @param rows rows to write
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void write(String table, List<Object[]> rows) throws MutationsRejectedException, TableNotFoundException {
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);
		BatchWriter writer = this.connector.createBatchWriter(table, config);

		for ( Object[] row : rows ) {
			Mutation mutation = new Mutation(row[0].toString());
			mutation.put(row[1].toString(), row[2].toString(), new ColumnVisibility(row[3].toString()), (long)row[4], (Value)row[5]);
			writer.addMutation(mutation);
		}

		writer.close();
	}
}