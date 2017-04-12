package com.mnsuk.uima;

import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.uima.UIMAFramework;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.apache.uima.util.XMLInputSource;

import com.mns.uima.utils.CASUtils;
import com.mns.uima.utils.DocumentDetails;
import com.mns.uima.utils.PrimitiveAFS;
import com.mnsuk.uima.db.SQLExecuter;
import com.mnsuk.uima.db.Session;

/**
 * FactExtract provides an externally configurable CAS consumer for persisting
 * annotations to a database.
 * <p>
 * @author      martin.saunders@uk.ibm.com
 */
public class FactExtract extends JCasAnnotator_ImplBase {
	
	public static final String MESSAGE_DIGEST = "com.mns.uima.FactExtract_Messages";
	
	public static final String USE_DEFAULT_CAS_VIEW = "-666"; // something no one will ever use

	// AE parameters
	private static final String PARAM_GROUP_JDBC = "JDBC";
	private static final String PARAM_JDBC_DBHOST = "DBHost";   
	private static final String PARAM_JDBC_DBPORT = "DBPort";   
	private static final String PARAM_JDBC_DBNAME = "DBName";   
	private static final String PARAM_JDBC_DBSCHEMA = "DBSchema";   
	private static final String PARAM_JDBC_DBUSER = "DBUser";   
	private static final String PARAM_JDBC_DBPASSWORD = "DBPassword";   
	private static final String PARAM_JDBC_DB = "DB";   
	
	private static final String PARAM_GROUP_GENERAL = "General";
	private static final String PARAM_GENERAL_PROJECT = "Project";
	private static final String PARAM_GENERAL_PREPEND = "PrependTableNames";
	private static final String PARAM_GENERAL_DROP = "DropTables";
	private static final String PARAM_GENERAL_SAVE_TEXT = "SaveDocText";
	private static final String PARAM_GENERAL_KEYFIELD = "KeyField";
	
	private static final int PROJECT_MAX_NAME_LEN = 10;
	
	// Global parameter values
	private static String pDBHost, pDBPort, pDBName, pDBSchema, pDBUser, pDBPassword, pDB;   
	private static String pProject, pKeyField;
	private static Boolean pDrop, pPrependTableNames, pSaveDocText;
	
	// Global Variables
	private Logger logger = null;
	private JCas jcas = null;
	private String text = null;
	private Session session;
	private SQLExecuter sqlx = null;
	
	/**
	 * Read configuration and initialise annotator with configuration.
	 * <p>
	 *
	 * @param  aContext 
	 * @throws ResourceInitializationException
	 */
	public void initialize(UimaContext aContext) throws ResourceInitializationException {
		super.initialize(aContext);
		logger = aContext.getLogger();	
		logger.log(Level.INFO, "FactExtract: initializing:");
		pDBHost = CASUtils.getConfigurationStringValue(aContext, PARAM_GROUP_JDBC, PARAM_JDBC_DBHOST).trim();
		pDBPort = CASUtils.getConfigurationStringValue(aContext, PARAM_GROUP_JDBC, PARAM_JDBC_DBPORT).trim();
		pDBName = CASUtils.getConfigurationStringValue(aContext, PARAM_GROUP_JDBC, PARAM_JDBC_DBNAME).trim();
		pDBSchema = CASUtils.getConfigurationStringValue(aContext, PARAM_GROUP_JDBC, PARAM_JDBC_DBSCHEMA).toUpperCase().trim();
		pDBUser = CASUtils.getConfigurationStringValue(aContext, PARAM_GROUP_JDBC, PARAM_JDBC_DBUSER).trim();
		pDBPassword = CASUtils.getConfigurationStringValue(aContext, PARAM_GROUP_JDBC, PARAM_JDBC_DBPASSWORD).trim();
		pDB = CASUtils.getConfigurationStringValue(aContext, PARAM_GROUP_JDBC, PARAM_JDBC_DB).trim();
		String tmp = CASUtils.getConfigurationStringValue(aContext, PARAM_GROUP_GENERAL, PARAM_GENERAL_PROJECT).toUpperCase().trim();
		pProject = (tmp.length() > PROJECT_MAX_NAME_LEN) ? tmp.substring(0, PROJECT_MAX_NAME_LEN) : tmp;
		pDrop = CASUtils.getConfigurationBooleanValue(aContext, PARAM_GROUP_GENERAL, PARAM_GENERAL_DROP);
		pPrependTableNames = CASUtils.getConfigurationBooleanValue(aContext, PARAM_GROUP_GENERAL, PARAM_GENERAL_PREPEND);
		pSaveDocText = CASUtils.getConfigurationBooleanValue(aContext, PARAM_GROUP_GENERAL, PARAM_GENERAL_SAVE_TEXT);
		pKeyField = CASUtils.getConfigurationStringValue(aContext, PARAM_GROUP_GENERAL, PARAM_GENERAL_KEYFIELD);
		
		session = new Session(pDB, pDBHost, pDBPort, pDBName, pDBSchema, pDBUser, pDBPassword);
		sqlx = new SQLExecuter(session, pProject, pDrop, pPrependTableNames);
	}
	
	public void collectionProcessComplete() throws AnalysisEngineProcessException {
		logger.log(Level.INFO, "FactExtract: Completed");
		try {
			session.close();
		} catch (SQLException e) {
			logger.log(Level.SEVERE, e.toString());
			throw new AnalysisEngineProcessException (
					AnalysisEngineProcessException.ANNOTATOR_EXCEPTION, new Object[] {e});
		}
	}
	
	public void batchProcessComplete() throws AnalysisEngineProcessException {
		logger.log(Level.INFO, "FactExtract: Batch completed");
		try {
			session.close();
		} catch (SQLException e) {
			logger.log(Level.SEVERE, e.toString());
			throw new AnalysisEngineProcessException (
					AnalysisEngineProcessException.ANNOTATOR_EXCEPTION, new Object[] {e});
		}
	}

	/** 
	 * @see org.apache.uima.analysis_component.JCasAnnotator_ImplBase#process(org.apache.uima.jcas.JCas)
	 */
	
	public void process(JCas aJCas) throws AnalysisEngineProcessException {
		logger.log(Level.INFO, "FactExtract: processing:");
		
	
		//ResourceBundle.getBundle(MESSAGE_DIGEST, java.util.Locale.getDefault());
		this.jcas=aJCas;
		if (pSaveDocText)
			this.text=jcas.getDocumentText();
		DocumentDetails.extractDocumentDetails(jcas);
		HashMap<String, ArrayList<String>> annoFtMap = sqlx.getAnnotationFeatures();
			
		String uniq = extractRecordID(pKeyField, jcas); // ok if returns null
		String doc_id=null;
		if (!annoFtMap.isEmpty())
			doc_id= sqlx.registerDocument(text, uniq); // writes to DOCUMENT table
		else
			logger.log(Level.INFO, "FactExtract: No annotations specified for persistence.");
		
		Iterator<Entry<String, ArrayList<String>>> itr = annoFtMap.entrySet().iterator();
		while (itr.hasNext()) {
			Map.Entry<String, ArrayList<String>> entry = (Map.Entry<String, ArrayList<String>>) itr.next();

			ArrayList<PrimitiveAFS> pafs = CASUtils.extractPrimitiveAFSList(jcas, entry.getKey(), entry.getValue());
			if (!pafs.isEmpty())
				sqlx.persistPrimitveAFSList(doc_id, pafs);
		}

		logger.log(Level.INFO, "FactExtract: processed document: " + DocumentDetails.title);
	}
	
	/**
	 * Returns the value of a metadata field aatached to the current document. 
	 * The assumption is that the requested field is a unique record identifier
	 * that can be used a primary key in the DOCUMENTS table.
	 *
	 * @param  JCas 
	 */
	private String extractRecordID(String fieldName, JCas jc) {
		String ret = null;
		
		Properties md = CASUtils.extractICAMetaFields(jc.getCas().getView("_InitialView"));
		String mdValue = (String) md.get(fieldName);
		if (mdValue != null && !mdValue.isEmpty())
			ret=mdValue.trim();
		
		return ret;
	}
	
	
	
	
	/**
	 * Test annotators process method outside of a UIMA pipeline. 
	 * Primary usage is debugging with eclipse.
	 * <p>
	 * Necessary xml and xmi files may be generated in ICA Studio using
	 * the "Save as XMI" feature after annotating a document.
	 *
	 * @param  typeFile xml typesytem file
	 * @param  casFile xmi of exported cas
	 */
	public void TestAnnotator(String typeFile, String casFile) {

		try {
			logger = UIMAFramework.getLogger(FactExtract.class);

			XMLInputSource xmlIn = new XMLInputSource(typeFile);
			TypeSystemDescription tsDesc = UIMAFramework.getXMLParser().parseTypeSystemDescription(xmlIn);
			CAS cas = CasCreationUtils.createCas( tsDesc, null, null ); 
			XmiCasDeserializer.deserialize(new FileInputStream(casFile), cas, false);			
			jcas = cas.getJCas();
			// in place of init
			session = new Session(pDB, pDBHost, pDBPort, pDBName, pDBSchema, pDBUser, pDBPassword);
			sqlx = new SQLExecuter(session, pProject, pDrop, pPrependTableNames);
			// and process
			process(jcas);
			collectionProcessComplete();
		}
		catch(Exception e) {
			logger.log(Level.SEVERE, "Error initialising the JCas: " + e.toString(),e);
		}
	}

	/**
	 * Setter for AE parameters used in debugging.
	 * <p>
	 * @param pExample
	 */
	public static void setpDBHost(String pExample) {
		FactExtract.pDBHost = pExample;
	}

	public static String getpDBPort() {
		return pDBPort;
	}

	public static void setpDBPort(String pDBPort) {
		FactExtract.pDBPort = pDBPort;
	}

	public static String getpDBName() {
		return pDBName;
	}

	public static void setpDBName(String pDBName) {
		FactExtract.pDBName = pDBName;
	}

	public static String getpDBSchema() {
		return pDBSchema;
	}

	public static void setpDBSchema(String pDBSchema) {
		FactExtract.pDBSchema = pDBSchema.toUpperCase();
	}

	public static String getpDBUser() {
		return pDBUser;
	}

	public static void setpDBUser(String pDBUser) {
		FactExtract.pDBUser = pDBUser;
	}

	public static String getpDBPassword() {
		return pDBPassword;
	}

	public static void setpDBPassword(String pDBPassword) {
		FactExtract.pDBPassword = pDBPassword;
	}
	
	public static String getpDB() {
		return pDB;
	}

	public static void setpDB(String pDB) {
		FactExtract.pDB = pDB;
	}
	
	public static String getpKeyField() {
		return pKeyField;
	}

	public static void setpKeyField(String pKeyField) {
		FactExtract.pKeyField = pKeyField;
	}
	
	public static String getpProject() {
		return pProject;
	}

	public static void setpProject(String pProject) {
		FactExtract.pProject = pProject.toUpperCase();
	}
	
	public static void setpDrop(Boolean pDrop) {
		FactExtract.pDrop = pDrop;
	}
	
	public static void setpPrependTableNames(Boolean pPrependTableNames) {
		FactExtract.pPrependTableNames = pPrependTableNames;
	}
	
	public static void setpSaveDocText(Boolean pSaveDocText) {
		FactExtract.pSaveDocText = pSaveDocText;
	}
	
}
