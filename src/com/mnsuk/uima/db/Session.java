package com.mnsuk.uima.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.uima.resource.ResourceInitializationException;


/**
 * Create and manage database connectivity.
 * <p>
 * @author      martin.saunders@uk.ibm.com
 */
public class Session {
	private Connection conn=null;
	private boolean closed=true;
	private String db, dbHost, dbPort, dbName, dbSchema, dbUser, dbPassword;
	static private final String DB2 = "DB2";
	static private final String MSSQL = "MSSQL";


	/**
	 * Create database session object and connect
	 * <p>
	 * @param DB  database type
	 * @param dbHost  database server host
	 * @param dbPort  database server listener port
	 * @param dbName  datbase name
	 * @param dbSchema database schema
	 * @param dbUser   database user to authenticate connection
	 * @param dbPassword  database user's password
	 * @throws ResourceInitializationException
	 */
	public Session(String DB, String dbHost, String dbPort, String dbName, String dbSchema, String dbUser, String dbPassword ) throws ResourceInitializationException {
		super();
		this.db = DB;
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.dbName = dbName;
        this.dbSchema = dbSchema;
        this.dbUser = dbUser; 
        this.dbPassword = dbPassword;   
		this.conn = connect();		
	}

	/**
	 * Connect to database.
	 * <p>
	 * @throws ResourceInitializationException
	 */
	private Connection connect()
			throws ResourceInitializationException {
		Connection connection;
		
		try {
			close();
			String url = null;
			if (db.equals(DB2)) {
				Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
				url = "jdbc:db2://" + this.dbHost + ":" + this.dbPort + "/" + this.dbName;  
			} else if (db.equals(MSSQL)) {
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
				url = "jdbc:sqlserver://" + this.dbHost + ":" + this.dbPort + ";databaseName=" + this.dbName;  			
			} else {
				throw new ResourceInitializationException (
						ResourceInitializationException.ANNOTATOR_INITIALIZATION_FAILED + " Invalid database type: " + db, new Object[] {});
			}
			connection = DriverManager.getConnection(url, this.dbUser, this.dbPassword);   
			this.closed = false;
		} catch (SQLException e) {
			throw new ResourceInitializationException (
					ResourceInitializationException.ANNOTATOR_INITIALIZATION_FAILED, new Object[] {e});
		} catch (IllegalAccessException e) {
			throw new ResourceInitializationException (
					ResourceInitializationException.ANNOTATOR_INITIALIZATION_FAILED, new Object[] {e});
		} catch (InstantiationException e) {
			throw new ResourceInitializationException (
					ResourceInitializationException.ANNOTATOR_INITIALIZATION_FAILED, new Object[] {e});
		} catch (ClassNotFoundException e) {
			throw new ResourceInitializationException (
					ResourceInitializationException.ANNOTATOR_INITIALIZATION_FAILED + " JDBC Driver not found.", new Object[] {e});
		}
		return connection;
	}
	
	/**
	 * Close database connection
	 * @throws SQLException
	 */
	public void close() throws SQLException {
		if (!this.closed) {
			try {
				this.conn.close();
			} finally {
				this.conn = null;
				this.closed = true;
			}
		}
	}
	
	/**
	 * Get database connection
	 * <p>
	 * @return database connection object
	 */
	public Connection getConnection() {
		return this.conn;
	}

	/**
	 * Get database name for current connection
	 * <p>
	 * @return database name
	 */
	public String getDbName() {
		return dbName;
	}

	/**
	 * Get Database type for current connection
	 * <p>
	 * @return type name
	 */
	public String getDb() {
		return db;
	}
	
	/**
	 * Get schema name for current connection
	 * <p>
	 * @return schema name
	 */
	public String getDbSchema() {
		return dbSchema;
	}

	/**
	 * Set default schema for current connection.
	 * <p>
	 * @param dbSchema
	 */
	public void setDbSchema(String dbSchema) {
		this.dbSchema = dbSchema;
	}
}
