package org.sensoriclife.db;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
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
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.sensoriclife.Logger;

/**
 *
 * @author jnphilipp
 * @version 0.2.0
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
	/**
	 * mini Accumulo cluster
	 */
	private MiniAccumuloCluster accumulo;
	/**
	 * batch writers
	 */
	private Map<String, BatchWriter> batchWriters;

	private Accumulo() {
		this.instance = null;
		this.connector = null;
		this.accumulo = null;
		this.batchWriters = new LinkedHashMap<>();
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
	public synchronized Scanner getScanner(String table) throws TableNotFoundException {
		return this.getScanner(table, Authorizations.EMPTY);
	}

	/**
	 * Returns a Scanner for the given table.
	 * @param table table
	 * @param visibility column visibility
	 * @return scanner
	 * @throws TableNotFoundException
	 */
	public synchronized Scanner getScanner(String table, String visibility) throws TableNotFoundException {
		return this.getScanner(table, Authorizations.EMPTY);
	}

	/**
	 * Returns a Scanner for the given table.
	 * @param table table
	 * @param auths authorizations
	 * @return scanner
	 * @throws TableNotFoundException
	 */
	public synchronized Scanner getScanner(String table, Authorizations auths) throws TableNotFoundException {
		return this.connector.createScanner(table, auths);
	}

	/**
	 * Connects to Accumulo using the MockIsntance.
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	public synchronized void connect() throws AccumuloException, AccumuloSecurityException {
		this.instance = new MockInstance("mockInstance");
		this.connector = this.instance.getConnector("",  new PasswordToken(""));
	}

	/**
	 * Connects to Accumulo using the MockIsntance with the given instanceName.
	 * @param instanceName instance name
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	public synchronized void connect(String instanceName) throws AccumuloException, AccumuloSecurityException {
		this.instance = new MockInstance(instanceName);
		this.connector = this.instance.getConnector("",  new PasswordToken(""));
	}

	/**
	 * Connects to Accumulo using the MockIsntance with the given instanceName.
	 * @param tmpDirectory temporary directory
	 * @param rootPassword root password
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public synchronized void connect(File tmpDirectory, String rootPassword) throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException {
		Logger.debug(Accumulo.class, "tmp directory: " + tmpDirectory);
		this.accumulo = new MiniAccumuloCluster(tmpDirectory, rootPassword);
		this.accumulo.start();

		this.instance = new ZooKeeperInstance(this.accumulo.getInstanceName(), this.accumulo.getZooKeepers());
		this.connector = this.instance.getConnector("root", new PasswordToken(rootPassword));
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
	 * returns the connector
	 * @return Connector
	 */
	public Connector getConnector() {
		return connector;
	}
	
	public MockInstance getMockInstance(){
		return (MockInstance) instance;
	}

	/**
	 * Disconnects and closes Accumulo.
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws MutationsRejectedException
	 */
	public synchronized void disconnect() throws IOException, InterruptedException, MutationsRejectedException {
		this.instance = null;
		this.connector = null;

		for ( BatchWriter writer : this.batchWriters.values() )
			writer.close();

		this.batchWriters.clear();

		if ( this.accumulo != null )
			this.accumulo.stop();
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
	 * Deletes the given table.
	 * @param table table name
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	public void deleteTable(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		this.connector.tableOperations().delete(table);
	}

	/**
	 * Flushes the batch writer for the given table.
	 * @param table table
	 * @throws MutationsRejectedException
	 */
	public void flushBashWriter(String table) throws MutationsRejectedException {
		Logger.debug(Accumulo.class, "Flushing bash writer for table: " + table);
		if ( this.batchWriters.containsKey(table) )
			this.batchWriters.get(table).flush();
	}

	/**
	 * Closes the batch writer for the given table.
	 * @param table table
	 * @throws MutationsRejectedException
	 */
	public void closeBashWriter(String table) throws MutationsRejectedException {
		Logger.debug(Accumulo.class, "Closing bash writer for table: " + table);

		if ( this.batchWriters.containsKey(table) ) {
			this.batchWriters.get(table).close();
			this.batchWriters.remove(table);
		}
	}

	/**
	 * Returns all elements of the given table.
	 * @param table table
	 * @return Iterator for all elements
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanAll(String table) throws TableNotFoundException {
		//return this.scanAll(table, Authorizations.EMPTY);
		return this.scanAll(table, new Authorizations());
	}

	/**
	 * Returns all elements of the given table.
	 * @param table table
	 * @param auths column visibility
	 * @return Iterator for all elements
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanAll(String table, String auths) throws TableNotFoundException {
		Authorizations a = new Authorizations(auths);
		return this.scanAll(table, a);
	}

	/**
	 * Returns all elements of the given table.
	 * @param table table
	 * @param auths column visibility
	 * @return Iterator for all elements
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanAll(String table, Authorizations auths) throws TableNotFoundException {
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
		return this.scanColumns(table, columnFamily, columnQualifier, Authorizations.EMPTY);
	}

	/**
	 * Returns all elements filter by the given column family and column qualifier.
	 * @param table table
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param auths column visibility
	 * @return iterator
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanColumns(String table, String columnFamily, String columnQualifier, String auths) throws TableNotFoundException {
		Authorizations a = new Authorizations(auths);
		return this.scanColumns(table, columnFamily, columnQualifier, a);
	}

	/**
	 * Returns all elements filter by the given column family and column qualifier.
	 * @param table table
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param auths column visibility
	 * @return iterator
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanColumns(String table, String columnFamily, String columnQualifier, Authorizations auths) throws TableNotFoundException {
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
		return this.scanByKey(table, Authorizations.EMPTY, range);
	}

	/**
	 * Returns all elements filter by the given range for the row ids.
	 * @param table table
	 * @param auths column visibility
	 * @param range row id range
	 * @return iterator
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanByKey(String table, String auths, Range range) throws TableNotFoundException {
		Authorizations a = new Authorizations(auths);
		return this.scanByKey(table, a, range);
	}

	/**
	 * Returns all elements filter by the given range for the row ids.
	 * @param table table
	 * @param auths column visibility
	 * @param range row id range
	 * @return iterator
	 * @throws TableNotFoundException
	 */
	public synchronized Iterator<Entry<Key,Value>> scanByKey(String table, Authorizations auths, Range range) throws TableNotFoundException {
		Scanner scan = this.connector.createScanner(table, auths);
		scan.setRange(range);
		Iterator<Entry<Key,Value>> iterator = scan.iterator();

		return iterator;
	}

	/**
	 * Adds the mutation to the batch writer.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void addMutation(String table, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		this.addMutation(table, rowId, columnFamily, columnQualifier, "", System.currentTimeMillis(), value);
	}

	/**
	 * Adds the mutation to the batch writer.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void addMutation(String table, String rowId, String columnFamily, String columnQualifier, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		this.addMutation(table, rowId, columnFamily, columnQualifier, "", System.currentTimeMillis(), new Value(value));
	}

	/**
	 * Adds the mutation to the batch writer.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void addMutation(String table, String rowId, String columnFamily, String columnQualifier, Value value) throws MutationsRejectedException, TableNotFoundException {
		this.addMutation(table, rowId, columnFamily, columnQualifier, "", System.currentTimeMillis(), value);
	}

	/**
	 * Adds the mutation to the batch writer.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param visibility column visibility
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void addMutation(String table, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, String visibility, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		this.addMutation(table, rowId, columnFamily, columnQualifier, visibility, System.currentTimeMillis(), value);
	}

	/**
	 * Adds the mutation to the batch writer.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param visibility column visibility
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void addMutation(String table, String rowId, String columnFamily, String columnQualifier, String visibility, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		this.addMutation(table, rowId, columnFamily, columnQualifier, visibility, System.currentTimeMillis(), new Value(value));
	}

	/**
	 * Adds the mutation to the batch writer.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param visibility column visibility
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void addMutation(String table, String rowId, String columnFamily, String columnQualifier, String visibility, Value value) throws MutationsRejectedException, TableNotFoundException {
		this.addMutation(table, rowId, columnFamily, columnQualifier, visibility, System.currentTimeMillis(), value);
	}

	/**
	 * Adds the mutation to the batch writer.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param timestamp timestamp
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void addMutation(String table, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, long timestamp, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		this.addMutation(table, rowId, columnFamily, columnQualifier, "", timestamp, value);
	}

	/**
	 * Adds the mutation to the batch writer.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param timestamp timestamp
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void addMutation(String table, String rowId, String columnFamily, String columnQualifier, long timestamp, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		this.addMutation(table, rowId, columnFamily, columnQualifier, "", timestamp, new Value(value));
	}

	/**
	 * Adds the mutation to the batch writer.
	 * @param table table
	 * @param rowId row id
	 * @param columnFamily column family
	 * @param columnQualifier column qualifier
	 * @param timestamp timestamp
	 * @param value value
	 * @throws MutationsRejectedException
	 * @throws TableNotFoundException
	 */
	public synchronized void addMutation(String table, String rowId, String columnFamily, String columnQualifier, long timestamp, Value value) throws MutationsRejectedException, TableNotFoundException {
		this.addMutation(table, rowId, columnFamily, columnQualifier, "", timestamp, value);
	}

	/**
	 * Adds the mutation to the batch writer.
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
	public synchronized void addMutation(String table, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, String visibility, long timestamp, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		if ( !this.batchWriters.containsKey(table) ) {
			BatchWriterConfig config = new BatchWriterConfig();
			config.setMaxMemory(10000000L);
			this.batchWriters.put(table, this.connector.createBatchWriter(table, config));
		}

		ColumnVisibility colVis = new ColumnVisibility(visibility);

		Mutation mutation = new Mutation(rowId);
		mutation.put(columnFamily, columnQualifier, colVis, timestamp, value);
		this.batchWriters.get(table).addMutation(mutation);
	}

	/**
	 * Adds the mutation to the batch writer.
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
	public synchronized void addMutation(String table, String rowId, String columnFamily, String columnQualifier, String visibility, long timestamp, Value value) throws MutationsRejectedException, TableNotFoundException {
		if ( !this.batchWriters.containsKey(table) ) {
			BatchWriterConfig config = new BatchWriterConfig();
			config.setMaxMemory(10000000L);
			this.batchWriters.put(table, this.connector.createBatchWriter(table, config));
		}

		ColumnVisibility colVis = new ColumnVisibility(visibility);

		Mutation mutation = new Mutation(rowId);
		mutation.put(columnFamily, columnQualifier, colVis, timestamp, value);
		this.batchWriters.get(table).addMutation(mutation);
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
	@Deprecated
	public synchronized void write(String table, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, "", System.currentTimeMillis(), value);
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
	@Deprecated
	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, String value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, "", System.currentTimeMillis(), value);
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
	@Deprecated
	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, Value value) throws MutationsRejectedException, TableNotFoundException {
		this.write(table, rowId, columnFamily, columnQualifier, "", System.currentTimeMillis(), value);
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
	@Deprecated
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
	@Deprecated
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
	@Deprecated
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
	@Deprecated
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
	@Deprecated
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
	@Deprecated
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
	@Deprecated
	public synchronized void write(String table, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, String visibility, long timestamp, byte[] value) throws MutationsRejectedException, TableNotFoundException {
		ColumnVisibility colVis = new ColumnVisibility(visibility);

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
	@Deprecated
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
	@Deprecated
	public synchronized void write(String table, String rowId, String columnFamily, String columnQualifier, String visibility, long timestamp, Value value) throws MutationsRejectedException, TableNotFoundException {
		ColumnVisibility colVis = new ColumnVisibility(visibility);

		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxMemory(10000000L);
		BatchWriter writer = this.connector.createBatchWriter(table, config);

		Mutation mutation = new Mutation(rowId);
		mutation.put(columnFamily, columnQualifier, colVis, timestamp, value);
		writer.addMutation(mutation);

		writer.close();
	}
}