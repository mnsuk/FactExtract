package com.mnsuk.uima.db;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

import com.mns.uima.utils.DocumentDetails;
import com.mns.uima.utils.PrimitiveAFS;
import com.mnsuk.uima.FactExtract;


/**
 * Interface to database
 * <p>
 * @author martin.saunders@uk.ibm.com
 *
 */

public class SQLExecuter {
	static private final String LAZYMODE_TRIGGER_FEATURE = "persist"; 
	static private final String CONFIG_TABLE = "CONFIGSCHEMA";
	// DB2 tablename max length is 128 characters, created table is project_tablename so max is 117
	static private String CREATE_CONFIG_DB2 =
			"CREATE TABLE $tableName (ROW_ID INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1, NO CACHE), "
			        + "PROJECT VARCHAR(10) NOT NULL, TABLENAME VARCHAR(117) NOT NULL, ANNOTYPE VARCHAR(128) NOT NULL, " 
					+ "COLUMNNAME VARCHAR(30), FEATURE VARCHAR(30), PRIMARY KEY (ROW_ID))";

	static private String CREATE_CONFIG_MSSQL =
			"CREATE TABLE $tableName (ROW_ID INT IDENTITY(1,1) PRIMARY KEY, "
			        + "PROJECT VARCHAR(10) NOT NULL, TABLENAME VARCHAR(117) NOT NULL, ANNOTYPE VARCHAR(128) NOT NULL, " 
					+ "COLUMNNAME VARCHAR(30), FEATURE VARCHAR(30))";
	
	static private String CREATE_CONFIG_MYSQL =
			"CREATE TABLE $tableName (ROW_ID INT AUTO_INCREMENT PRIMARY KEY, "
			        + "PROJECT VARCHAR(10) NOT NULL, TABLENAME VARCHAR(117) NOT NULL, ANNOTYPE VARCHAR(128) NOT NULL, " 
					+ "COLUMNNAME VARCHAR(30), FEATURE VARCHAR(30))";
	
	static private String CREATE_CONFIG = null;

	static private String SEL_TABLES =
			"SELECT DISTINCT tablename, annotype FROM $tableName "
					+ "WHERE UPPER(project) = ?";
	static private final int SEL_TABLES_TABLENAME = 1;
	static private final int SEL_TABLES_ANNOTYPE = 2;

	private String SEL_ANNOS =
			"SELECT DISTINCT annotype, feature FROM $tableName "
					+ "WHERE UPPER(project) = ? ORDER BY annotype";
	static private final int SEL_ANNOS_ANNOTYPE = 1;
	static private final int SEL_ANNOS_FEATURE = 2;

	static private String SEL_FEATURES =
			"SELECT columnname, feature FROM $tableName "
					+ " WHERE UPPER(project) = ? AND UPPER(tablename) = ? ORDER BY columnname";
	static private final int SEL_FEATURES_COLUMNNAME = 1;
	static private final int SEL_FEATURES_FEATURE = 2;

	static private String INS_START_COLUMNS = "DOC_ID, COVERED_TEXT, BEGIN_OFFSET, END_OFFSET";
	static private final int INS_START_COLUMN_COUNT = 4;
	static private final String INS_END_COLUMNS = "INSERTION_TS";
	static private final int INS_END_COLUMN_COUNT = 1;
	static private final int INS_DOC_ID = 1;
	static private final int INS_COVERED_TEXT = 2;
	static private final int INS_BEGIN_OFFSET = 3;
	static private final int INS_END_OFFSET = 4;

	static private final String CREATE_SCHEMA = "CREATE SCHEMA ";
	static private final String DOCUMENT_TABLE = "DOCUMENTS";
	static private final int CREATE_DOC_URL_LEN = 256;
	static private final int CREATE_DOC_TITLE_LEN = 50;
	static private final int CREATE_DOC_DATASOURCE_NAME_LEN = 50;
	static private final int CREATE_DOC_DOCTEXT_LEN = 100000;

	
	static private String CREATE_DOCUMENT_DB2 =
			"CREATE TABLE $tableName (DOC_ID  VARCHAR(50) NOT NULL, URL VARCHAR(" + CREATE_DOC_URL_LEN +
			") NOT NULL, TITLE VARCHAR(" + CREATE_DOC_TITLE_LEN + ") NOT NULL, DATASOURCE_NAME VARCHAR(" +
			CREATE_DOC_DATASOURCE_NAME_LEN + ") NOT NULL, DOC_DATE TIMESTAMP, DOCTEXT CLOB(" +
			CREATE_DOC_DOCTEXT_LEN + "), UPDATE_TS TIMESTAMP NOT NULL, PRIMARY KEY (DOC_ID))";

	static private String CREATE_DOCUMENT_MSSQL =
			"CREATE TABLE $tableName (DOC_ID  VARCHAR(50) NOT NULL, URL VARCHAR(" + CREATE_DOC_URL_LEN +
			") NOT NULL, TITLE VARCHAR(" + CREATE_DOC_TITLE_LEN + ") NOT NULL, DATASOURCE_NAME VARCHAR(" +
			CREATE_DOC_DATASOURCE_NAME_LEN + ") NOT NULL, DOC_DATE DATETIME, DOCTEXT TEXT, UPDATE_TS DATETIME NOT NULL)";

	static private String CREATE_DOCUMENT_MYSQL =
			"CREATE TABLE $tableName (DOC_ID  VARCHAR(50) NOT NULL, URL VARCHAR(" + CREATE_DOC_URL_LEN +
			") NOT NULL, TITLE VARCHAR(" + CREATE_DOC_TITLE_LEN + ") NOT NULL, DATASOURCE_NAME VARCHAR(" +
			CREATE_DOC_DATASOURCE_NAME_LEN + ") NOT NULL, DOC_DATE DATETIME, DOCTEXT TEXT, UPDATE_TS DATETIME NOT NULL)";

	static private String CREATE_DOCUMENT = null;

	static private String SEL_DOCUMENT =
			"SELECT DOC_ID FROM $tableName WHERE DOC_ID=?";

	static private final int ANNO_TABLES_COLUMN_LENGTH = 50;
	static private final int ANNO_TABLES_COVEREDTEXT_LENGTH = 1000;
	static private final String CREATE_ANNO_TABLES_START_COLUMNS = "DOC_ID VARCHAR(30), COVERED_TEXT VARCHAR(" +
			ANNO_TABLES_COVEREDTEXT_LENGTH + "), BEGIN_OFFSET INTEGER, END_OFFSET INTEGER";
	static private final String CREATE_ANNO_TABLES_END_COLUMNS_DB2 = "INSERTION_TS TIMESTAMP";
	static private final String CREATE_ANNO_TABLES_END_COLUMNS_MSSQL = "INSERTION_TS DATETIME";
	static private final String CREATE_ANNO_TABLES_END_COLUMNS_MYSQL = "INSERTION_TS DATETIME";
	static private String CREATE_ANNO_TABLES_END_COLUMNS = null;
	static private final String CREATE_ANNO_TABLES_DEFAULT_DATATYPE = "VARCHAR(" + ANNO_TABLES_COLUMN_LENGTH + ")";
	static private final String CREATE_ANNO_TABLES_ROW_ID_DB2 = " (ROW_ID INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1, NO CACHE),";
	static private final String CREATE_ANNO_TABLES_ROW_ID_MSSQL = " (ROW_ID INT IDENTITY(1,1),";
	static private final String CREATE_ANNO_TABLES_ROW_ID_MYSQL = " (ROW_ID INT AUTO_INCREMENT,";
	static private String CREATE_ANNO_TABLES_ROW_ID = null;

	

	static private String INS_DOCUMENT_NO_TEXT =
			"INSERT INTO $tableName (DOC_ID, URL, TITLE, DATASOURCE_NAME, UPDATE_TS) VALUES ( ?, ?, ?, ?, ?)";
	static private String INS_DOCUMENT_WITH_TEXT =
			"INSERT INTO $tableName (DOC_ID, URL, TITLE, DATASOURCE_NAME, UPDATE_TS, DOCTEXT) VALUES ( ?, ?, ?, ?, ?, ?)";
	static private final int INS_DOCUMENT_DOC_ID = 1;
	static private final int INS_DOCUMENT_URL = 2;
	static private final int INS_DOCUMENT_TITLE = 3;
	static private final int INS_DOCUMENT_DATASOURCE_NAME = 4;
	static private final int INS_DOCUMENT_UPDATE_TS = 5;
	static private final int INS_DOCUMENT_DOCTEXT = 6;


	static private String UPD_DOCUMENT =
			"UPDATE $tableName SET UPDATE_TS=? WHERE DOC_ID=?";
	static private final int UPD_DOCUMENT_UPDATE_TS = 1;
	static private final int UPD_DOCUMENT_DOC_ID = 2;

	private Connection	conn = null;
	private String schemaName;
	private String configTable, documentTable;
	private String project = null;
	private Boolean dropTables, prepend;
	private Logger logger = UIMAFramework.getLogger(SQLExecuter.class);
	// annoTables associates and annotation with a database table <annotationname, tablename>
	private HashMap<String, String> annoTables = new HashMap<String, String>();
	// tableColumns  associates feature:columns (1:1) and to a tablename  <tablename, <featurename,columnname>>
	private HashMap<String, HashMap<String,String>> tableColumns = new HashMap<String, HashMap<String,String>>();
	// annoFeatures associates and annotation with its features to be persisted <annotation, <features>>
	private HashMap<String, ArrayList<String>> annoFeatures = new HashMap<String, ArrayList<String>>();

	public SQLExecuter(Session session, String project, Boolean drop, Boolean prepend, Boolean lazymode) throws ResourceInitializationException {
		this.conn = session.getConnection();
		this.schemaName = session.getDbSchema();
		this.configTable = schemaName + "." + CONFIG_TABLE;
		this.documentTable = schemaName + "." + DOCUMENT_TABLE;
		
		if (session.getDb().equals("DB2")) {
			CREATE_CONFIG = CREATE_CONFIG_DB2.replace("$tableName", configTable);
			CREATE_DOCUMENT = CREATE_DOCUMENT_DB2.replace("$tableName", documentTable);
			CREATE_ANNO_TABLES_END_COLUMNS = CREATE_ANNO_TABLES_END_COLUMNS_DB2;
			CREATE_ANNO_TABLES_ROW_ID = CREATE_ANNO_TABLES_ROW_ID_DB2;
		} else if (session.getDb().equals("MSSQL")) {
			CREATE_CONFIG = CREATE_CONFIG_MSSQL.replace("$tableName", configTable);
			CREATE_DOCUMENT = CREATE_DOCUMENT_MSSQL.replace("$tableName", documentTable);
			CREATE_ANNO_TABLES_END_COLUMNS = CREATE_ANNO_TABLES_END_COLUMNS_MSSQL;
			CREATE_ANNO_TABLES_ROW_ID = CREATE_ANNO_TABLES_ROW_ID_MSSQL;
		} else if (session.getDb().equals("MYSQL")) {
			CREATE_CONFIG = CREATE_CONFIG_MYSQL.replace("$tableName", configTable);
			CREATE_DOCUMENT = CREATE_DOCUMENT_MYSQL.replace("$tableName", documentTable);
			CREATE_ANNO_TABLES_END_COLUMNS = CREATE_ANNO_TABLES_END_COLUMNS_MYSQL;
			CREATE_ANNO_TABLES_ROW_ID = CREATE_ANNO_TABLES_ROW_ID_MYSQL;
		} else {
			CREATE_CONFIG = CREATE_CONFIG_DB2.replace("$tableName", configTable);
			CREATE_DOCUMENT = CREATE_DOCUMENT_DB2.replace("$tableName", documentTable);
			CREATE_ANNO_TABLES_END_COLUMNS = CREATE_ANNO_TABLES_END_COLUMNS_DB2;
			CREATE_ANNO_TABLES_ROW_ID = CREATE_ANNO_TABLES_ROW_ID_DB2;
		}
		SEL_TABLES = SEL_TABLES.replace("$tableName", configTable);
		SEL_FEATURES = SEL_FEATURES.replace("$tableName", configTable);
		SEL_ANNOS = SEL_ANNOS.replace("$tableName", configTable);
		SEL_DOCUMENT = SEL_DOCUMENT.replace("$tableName", documentTable);
		UPD_DOCUMENT = UPD_DOCUMENT.replace("$tableName", documentTable);
		INS_DOCUMENT_WITH_TEXT = INS_DOCUMENT_WITH_TEXT.replace("$tableName", documentTable);
		INS_DOCUMENT_NO_TEXT = INS_DOCUMENT_NO_TEXT.replace("$tableName", documentTable);
		this.project = project;
		this.dropTables = drop;
		this.prepend = prepend;
		checkConfig();
		readSchema();
		createSchema(lazymode);
	}

	/**
	 * Registers the document in the target database
	 * <p>
	 * @param pSaveDocText If true then full document text is persisted.
	 * @return primary key of persisted document reference
	 * @throws AnalysisEngineProcessException
	 */
	public String registerDocument(String doc_text, String useId) throws AnalysisEngineProcessException {
		PreparedStatement selStatement=null;
		PreparedStatement updStatement=null;
		
		String doc_id=null;
		if (useId==null) {
			int hash = DocumentDetails.url.hashCode();
			doc_id=Integer.toString(hash);
		} else
			doc_id=useId;
		
		String title = (DocumentDetails.title.length() > CREATE_DOC_TITLE_LEN) 
				? DocumentDetails.title.substring(0, CREATE_DOC_TITLE_LEN) 
						: DocumentDetails.title;
		try {
			selStatement = conn.prepareStatement(SEL_DOCUMENT);
			selStatement.setString(1, doc_id);
			ResultSet rs = selStatement.executeQuery();
			if (rs.next()) { // document exists update timestamp
				updStatement = conn.prepareStatement(UPD_DOCUMENT);
				updStatement.setTimestamp(UPD_DOCUMENT_UPDATE_TS, new Timestamp((new Date()).getTime()));
				updStatement.setString(UPD_DOCUMENT_DOC_ID, doc_id);
				logger.log(Level.INFO, "Updating document: " + doc_id);
			} else {  // new document
				String url = (DocumentDetails.url.length() > CREATE_DOC_URL_LEN) 
						? DocumentDetails.url.substring(DocumentDetails.url.length() - CREATE_DOC_URL_LEN) 
								: DocumentDetails.url;	
								String  dataSourceName = (DocumentDetails.dataSourceName.length() > CREATE_DOC_DATASOURCE_NAME_LEN) 
										? DocumentDetails.dataSourceName.substring(0, CREATE_DOC_DATASOURCE_NAME_LEN) 
												: DocumentDetails.dataSourceName;
										Date  docDate = DocumentDetails.docDate;
												logger.log(Level.INFO, "id:" + doc_id + " url: " + url + " title: " + title + " dataSourceName: " + dataSourceName + " docDate: " + docDate.toString());
												if (doc_text != null)
													updStatement = conn.prepareStatement(INS_DOCUMENT_WITH_TEXT);
												else
													updStatement = conn.prepareStatement(INS_DOCUMENT_NO_TEXT);
												updStatement.setString(INS_DOCUMENT_DOC_ID, doc_id);
												updStatement.setString(INS_DOCUMENT_URL, url);
												updStatement.setString(INS_DOCUMENT_TITLE, title);
												updStatement.setString(INS_DOCUMENT_DATASOURCE_NAME, dataSourceName);
												updStatement.setTimestamp(INS_DOCUMENT_UPDATE_TS, new Timestamp((new Date()).getTime()));
												if (doc_text != null) {
													StringReader reader = new StringReader(doc_text);
													updStatement.setCharacterStream(INS_DOCUMENT_DOCTEXT, reader, doc_text.length());
												} 
			}
			updStatement.executeUpdate();
		} catch (SQLException e) {
				// Resource localisation seems to be broken in Studio and ICA pipeline so log and throw
				
					logger.log(Level.SEVERE, "Registering document " + title + ", id " + doc_id + " in documents table.");
				throw new AnalysisEngineProcessException(FactExtract.MESSAGE_DIGEST, "register_document_error",
				                new Object[] { title, doc_id }, e);
		}
		finally {
			closeQuery(selStatement, (ResultSet) null);
			closeQuery(updStatement, (ResultSet) null);
		}
		return doc_id;
	}

	/**
	 * Reads the configuration table to determine the specified target schema.
	 * <p>
	 * @throws AnalysisEngineProcessException
	 */
	private void readSchema() throws ResourceInitializationException {
		PreparedStatement statement = null;
		ResultSet rs = null;

		try {
			// get tables to be created for each annotation
			statement = conn.prepareStatement(SEL_TABLES); 
			statement.setString(1, project);
			rs = statement.executeQuery();
			while (rs.next()) {
				String anno = rs.getString(SEL_TABLES_ANNOTYPE);
				anno = anno.trim(); // can't be null 
				if (!anno.isEmpty()) {
					String table = rs.getString(SEL_TABLES_TABLENAME);
					table = table.trim().toUpperCase(); // can't be null
					if (!table.isEmpty())
						annoTables.put(anno, table);	
				}
			}
			//rs.close();
			closeQuery(statement, rs);

			// get the features to be persisted for each annotation
			statement = conn.prepareStatement(SEL_ANNOS);
			statement.setString(1, project);
			rs = statement.executeQuery();
			String prevAnno = "";
			ArrayList<String> features = new ArrayList<String>();
			while (rs.next()) {
				String annoName = rs.getString(SEL_ANNOS_ANNOTYPE);
				if (annoName.equals(prevAnno)) {
					String ft = rs.getString(SEL_ANNOS_FEATURE);
					if (!rs.wasNull()){
						ft = ft.trim();
						if (!ft.isEmpty())
							features.add(ft);
					}
				} else {
					if (!prevAnno.isEmpty()) { // not the first
						annoFeatures.put(prevAnno, features);
						features = new ArrayList<String>();
					}
					String ft = rs.getString(SEL_ANNOS_FEATURE);
					if (!rs.wasNull()) {
						ft = ft.trim();
						if (!ft.isEmpty())
							features.add(ft);
					}
					prevAnno = annoName;
				}
			}
			if (!prevAnno.isEmpty())
				annoFeatures.put(prevAnno, features);
			closeQuery(statement, rs);
			//rs.close();

			// get the columns to be created for each feature
			Iterator<Entry<String, String>> itr = annoTables.entrySet().iterator();
			while (itr.hasNext()) {
				Map.Entry<String,String> entry = (Map.Entry<String,String>) itr.next();
				String table = entry.getValue();
				String anno = entry.getKey();
				HashMap<String,String> featureColumns = new HashMap<String,String>();
				statement = conn.prepareStatement(SEL_FEATURES);
				statement.setString(1,project);
				statement.setString(2, table);
				rs = statement.executeQuery();
				while (rs.next()) {
					String ft = rs.getString(SEL_FEATURES_FEATURE);
					if (!rs.wasNull()) {
						ft = ft.trim();
						if (!ft.isEmpty()) {
							String col = rs.getString(SEL_FEATURES_COLUMNNAME);
							if (!rs.wasNull() ) {
								col = col.trim().toUpperCase();
								if (!col.isEmpty())
									featureColumns.put(ft, col);
								else
									removeAnnoFeatureListItem(anno, ft);
							} else {
								removeAnnoFeatureListItem(anno, ft);			
							}
						}
					}

				}
				tableColumns.put(table,featureColumns);
			}		
		} catch (SQLException e) {
			// Resource localisation seems to be broken in Studio and ICA pipeline so log and throw
			
				logger.log(Level.SEVERE, "Reading configuration tables in schema " + schemaName + "."); 
	        throw new ResourceInitializationException(FactExtract.MESSAGE_DIGEST, "read_config_error",
	                new Object[] { schemaName }, e);
			
		} finally {
			closeQuery(statement, rs);
		}
	}
	
	/**
	 * Check if configuragtion tables exist and create if needed.
	 * <p>
	 * @throws AnalysisEngineProcessException
	 */
	private void checkConfig() throws ResourceInitializationException {
		PreparedStatement statement = null;
		ResultSet tables = null;

		try {
			DatabaseMetaData dbm = conn.getMetaData();

			tables = dbm.getTables(null, schemaName, CONFIG_TABLE, null);				
			if (!tables.next()) {
				statement = conn.prepareStatement(CREATE_CONFIG);
				statement.executeUpdate();
			}
		} catch (SQLException e) {
			// Resource localisation seems to be broken in Studio and ICA pipeline so log and throw
			
				logger.log(Level.SEVERE, "Checking config tables in schema " + schemaName + "." );
        throw new ResourceInitializationException(FactExtract.MESSAGE_DIGEST, "check_config_error",
	                new Object[] { schemaName }, e);
		}      
		finally {
			closeQuery(statement, (ResultSet) tables);
		}
	}

	/**
	 * Creates the specified target schema as necessary.
	 * <p>
	 * @throws AnalysisEngineProcessException
	 */
	private void createSchema(Boolean lazymode) throws ResourceInitializationException {
		PreparedStatement statement = null;

		try {		
			DatabaseMetaData dbm = conn.getMetaData();
			ResultSet schema = dbm.getTables(null, schemaName, null, null);				
			if (!schema.next()) {
				statement = conn.prepareStatement(CREATE_SCHEMA + schemaName);
				statement.executeUpdate();
				statement.close();
			}
			ResultSet tables = dbm.getTables(null, schemaName, DOCUMENT_TABLE, null);				
			if (!tables.next()) {
				statement = conn.prepareStatement(CREATE_DOCUMENT);
				statement.executeUpdate();
				statement.close();
			}

			Iterator<Entry<String, HashMap<String,String>>> itr = tableColumns.entrySet().iterator();
			while (itr.hasNext()) {
				Map.Entry<String,HashMap<String,String>> entry = (Map.Entry<String,HashMap<String,String>>) itr.next();
				String tableName; 
				if (prepend) 
					tableName = project.toUpperCase() + "_" + entry.getKey();
				else
					tableName = entry.getKey();
				if (dropTables) {
					tables = dbm.getTables(null, schemaName, tableName, null);				
					if (tables.next()) {
						try {
							statement = conn.prepareStatement("DROP TABLE " +  schemaName + "." + tableName);
							statement.executeUpdate();
							statement.close();
						} catch (SQLException e) {
							if (e.getErrorCode()!=-204 || !e.getSQLState().equals("42704"))
								throw e;
						}
					}
				} else {				
					tables = dbm.getTables(null, schemaName, tableName, null);				
					if (tables.next()) 
						continue;  // table already exist
				}

				StringBuffer sb = new StringBuffer();
				sb.append("CREATE TABLE ");
				sb.append(schemaName);
				sb.append(".");
				sb.append(tableName);
				//sb.append(" (");
				sb.append(CREATE_ANNO_TABLES_ROW_ID);
				sb.append(CREATE_ANNO_TABLES_START_COLUMNS);
				sb.append(", ");
				HashMap<String,String> columns = entry.getValue();
				Iterator<String> columnItr = columns.values().iterator();
				while (columnItr.hasNext()) {
					String columnName = columnItr.next();
					if (!(lazymode && columnName.equalsIgnoreCase(LAZYMODE_TRIGGER_FEATURE))) { // don't create lazymode feature trigger column
						sb.append(columnName);
						sb.append(" ");
						sb.append(CREATE_ANNO_TABLES_DEFAULT_DATATYPE);
						sb.append(", ");
					}
				}
				sb.append(CREATE_ANNO_TABLES_END_COLUMNS);
				sb.append(", PRIMARY KEY (ROW_ID))");
				String createSQL = sb.toString();
				statement = conn.prepareStatement(createSQL);
				statement.executeUpdate();
				statement.close();
			}

		} catch (SQLException e) {
			// Resource localisation seems to be broken in Studio and ICA pipeline so log and throw
			
				logger.log(Level.SEVERE, "Creating configuration tables in schema " + schemaName + "." );
	        throw new ResourceInitializationException(FactExtract.MESSAGE_DIGEST, "schema_create_error",
	                new Object[] { schemaName }, e);

		}
		finally {
			closeQuery(statement, (ResultSet) null);
		}

	}

	public void persistPrimitveAFSList(String doc_id, ArrayList<PrimitiveAFS> pafs, Boolean lazymode) throws AnalysisEngineProcessException {
		String annoName = pafs.get(0).getTypeStr();
		logger.log(Level.INFO, "Persisting annotation: " + annoName + " DOC_ID: " + doc_id);

		ArrayList<String> features = annoFeatures.get(annoName);
		String tgtTable = annoTables.get(annoName);
		String fqTgtTable;
		if (prepend)
			fqTgtTable = schemaName + "." + project + "_" + tgtTable;
		else
			fqTgtTable = schemaName + "." + tgtTable;
		if (lazymode) { // tables may not have been created as schema created in initilaisation when type system not available
			try {
				createSchema(lazymode); // does nothing if tables already exist
			} catch (ResourceInitializationException e) {
				logger.log(Level.SEVERE, "Error creating target schema in lazymode."); 
		        throw new AnalysisEngineProcessException(FactExtract.MESSAGE_DIGEST, "anno_persist_error",
		                new Object[] { "Creating target schema in lazymode - " + e.toString()}, e);
			}  
		}
		deleteEntries(fqTgtTable, doc_id);
		HashMap<String, String> tgtFtColumnMap = tableColumns.get(tgtTable);

		StringBuffer sb = new StringBuffer("INSERT INTO " + fqTgtTable + " (" + INS_START_COLUMNS);
		for (String ft : features) {
			if (!(lazymode && ft.equals(LAZYMODE_TRIGGER_FEATURE))) {  // don't persist lazymode trigger feature
				String col = tgtFtColumnMap.get(ft);
				if (col!=null && !col.isEmpty()) {
					sb.append(", ");
					sb.append(col);
				}
			}
		}
		sb.append(", ");
		sb.append(INS_END_COLUMNS);  
		sb.append(") VALUES(");
		for (int i=0; i < INS_START_COLUMN_COUNT; i++)
			sb.append("?, ");
		int featureCount = lazymode ? features.size() - 1 : features.size();
		for (int i=0; i < featureCount; i++) {	
			sb.append("?, ");
		}
		for (int i=0; i < INS_END_COLUMN_COUNT - 1; i++)
			sb.append("?, ");
		sb.append("?)"); 
		String insSQL = sb.toString();

		PreparedStatement statement=null;
		try {
			statement = conn.prepareStatement(insSQL);

			for (PrimitiveAFS paf : pafs) {
				HashMap<String, String> ftValues = paf.getFeatures();
				if (lazymode && (ftValues.get(LAZYMODE_TRIGGER_FEATURE) == null ||  
						(ftValues.get(LAZYMODE_TRIGGER_FEATURE) != null && ftValues.get(LAZYMODE_TRIGGER_FEATURE).equalsIgnoreCase("FALSE")))) {
					continue; // skip this annotation
				}					
				int i = 0, j = 0; // need two counters as we skip one feature in lazymode
				statement.setString(INS_DOC_ID, doc_id);
				String ct = paf.getCoveredText();
				if (ct.length() > ANNO_TABLES_COVEREDTEXT_LENGTH) 
					ct = ct.substring(0, ANNO_TABLES_COVEREDTEXT_LENGTH - "(truncated)".length()) + "(truncated)";		
				statement.setString(INS_COVERED_TEXT, ct);
				statement.setInt(INS_BEGIN_OFFSET, paf.getBegin());
				statement.setInt(INS_END_OFFSET, paf.getEnd());
				while (i < features.size()) {
					if (!(lazymode && features.get(i).equals(LAZYMODE_TRIGGER_FEATURE))) { // don't persist the lazymode trigger feature. 
						String value = ftValues.get(features.get(i));
						if (value==null)
							value="";
						if (value.length() > ANNO_TABLES_COLUMN_LENGTH)
							value = value.substring(0, ANNO_TABLES_COLUMN_LENGTH - "(truncated)".length()) + "(truncated)";
						statement.setString(j + INS_START_COLUMN_COUNT + 1,value);  // Columns before features + offset numbers from 1 not zero.
						j++;
					}
					i++;
				}
				statement.setTimestamp(j + INS_START_COLUMN_COUNT + 1, new Timestamp((new Date()).getTime()));
				statement.executeUpdate();
			}
		} catch (SQLException e) {
			// Resource localisation seems to be broken in Studio and ICA pipeline so log and throw
			
			logger.log(Level.SEVERE, "xxxPersisting annotation with SQL \"" + insSQL + "\"."); 
	        throw new AnalysisEngineProcessException(FactExtract.MESSAGE_DIGEST, "anno_persist_error",
	                new Object[] { insSQL }, e);

		}
		finally {
			closeQuery(statement, (ResultSet) null);
		}



	}

	/**
	 * Deletes all entries in a table for a given document
	 * <p>
	 * @param fqTgtTable  fully qualified table name
	 * @param doc_id  document key
	 * @throws AnalysisEngineProcessException 
	 */
	private void deleteEntries(String fqTgtTable, String doc_id) throws AnalysisEngineProcessException {
		PreparedStatement statement = null;
		String sql = "DELETE FROM " + fqTgtTable + " WHERE DOC_ID=?";
		try {
			statement = conn.prepareStatement(sql);
			statement.setString(1, doc_id);
			statement.execute();
		} catch (SQLException e) {
			// Resource localisation seems to be broken in Studio and ICA pipeline so log and throw
			
				logger.log(Level.SEVERE, "Delete entries for document id " + doc_id + " from table " + fqTgtTable + "."); 
	        throw new AnalysisEngineProcessException(FactExtract.MESSAGE_DIGEST, "delete_entries_error",
	                new Object[] { doc_id, fqTgtTable }, e);
		}
		finally {
			closeQuery(statement, (ResultSet) null);
		}

	}
	

	/**
	 * Close query statement and result set
	 * <p>
	 * @param statement
	 * @param rs
	 */
	private void closeQuery(PreparedStatement statement, ResultSet rs) {
		if ( rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Errors closing SQL result set. " + e.toString(), e);
			}
		}
		if (statement != null) {
			try {
				statement.close();
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Errors closing SQL statement. " + e.toString(), e);
			}
		}
	}

	private void removeAnnoFeatureListItem(String anno, String ft) {
		ArrayList<String> fts = annoFeatures.get(anno);
		int removeIdx = 666;
		boolean found=false;
		for (int i=0; i < fts.size(); i++) {
			if (fts.get(i).equals(ft)) {
				removeIdx = i;
				found=true;
				break;
			}		
		}
		if (found)
			fts.remove(removeIdx);
	}

	/**
	 * Iterates through all user types in the typesystem 
	 * to identify those that have been marked for persistence.
	 * <p>
	 * Retunrs a list of those types.
	 *
	 * @param  jc JCas
	 */
	public HashMap<String, ArrayList<String>> initPersistTypeList(JCas jc) {
		HashMap<String, ArrayList<String>> aF = new HashMap<String, ArrayList<String>>();
		List<Type> pTypes = new ArrayList<Type>();
		TypeSystem typeSystem = jc.getTypeSystem();
		Iterator typeIterator = typeSystem.getTypeIterator();
		Type t;
		while (typeIterator.hasNext()) {
			t = (Type) typeIterator.next();
			if (!t.getName().startsWith("uima.")) {
				List<Feature> fts = t.getFeatures();
				if (t.getFeatureByBaseName(LAZYMODE_TRIGGER_FEATURE)!=null) {
					if (!t.getName().contains(".en.")) {
						String typeName = t.getShortName();
						// assume target table name is same as annotation name
						annoTables.put(t.getName(), typeName.toUpperCase());
						ArrayList<String> featureNames = new ArrayList<String>();	
						HashMap<String,String> featureColumns = new HashMap<String,String>();
						for (Feature ft : fts){
							String ftName = ft.getShortName();
							if (!ftName.equals("sofa") && !ftName.equals("ruleId") && !ftName.equals("begin") && !ftName.equals("end")) {
								featureNames.add(ftName);
								// assume table columnnames are the same as feature names
								featureColumns.put(ftName, ftName.toUpperCase());
							}
							tableColumns.put(typeName.toUpperCase(),featureColumns);
						}
						aF.put(t.getName(), featureNames);
					}
				}
			}
		}
		annoFeatures = aF;
		return aF;
	}
	
	public HashMap<String, ArrayList<String>> getAnnotationFeatures() {
		return annoFeatures;
	}


}
