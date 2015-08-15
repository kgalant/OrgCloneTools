package com.salesforce.migrationtools.dbinventory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import com.salesforce.migrationtools.ObjectSFData;
import com.salesforce.migrationtools.Utils;

public class DBOps {
	private static Properties dbProperties;
	
	private static Connection myConnection;
	
	private static PreparedStatement tableExistsCheckStatement = null;
	private static PreparedStatement columnExistsCheckStatement = null;
	private static PreparedStatement insertObjectDataStatement = null;
	private static PreparedStatement deleteObjectDataStatement = null;
	private static PreparedStatement getFieldDefinitionsStatement = null;
	
	private static Statement columnCreateStatement = null; 
	private static Statement tableCreateStatement = null; 
	
	
	private static final Logger logger = LogManager.getLogger("MyLogger");
	
	private static final String tableCheckStatementSQL = "SELECT to_regclass(?)";
	private static final String columnCheckStatementSQL = "SELECT column_name FROM information_schema.columns WHERE table_name=? and column_name=?";
	private static final String columnCreateStatementSQL = "ALTER TABLE $$1$$ ADD COLUMN $$2$$";
	private static final String tableCreateStatementSQL = "CREATE TABLE IF NOT EXISTS $$1$$ ( ) WITH (OIDS=FALSE)";
	private static final String insertObjectDataSQL = "INSERT INTO fieldinventory (orgid, objectname, metadatadescription, partnerdescription) VALUES (?,?,?,?)";
	private static final String deleteObjectDataSQL = "DELETE FROM fieldinventory WHERE orgid=? AND objectname=?";
	
	private static final String getFieldDefinitionsSQL= 
			"SELECT objectname, fieldname, fieldJSON->>'type' fieldtype, fieldJSON->>'length' fieldlength, fieldJSON->>'scale' fieldscale FROM (" + 
			"SELECT coalesce(q1.objectname, q2.objectname) objectname, coalesce(q1.fieldName, q2.fieldName) fieldname, jsonb_append(q1.fieldJSON::jsonb, q2.fieldJSON::jsonb) fieldJSON" +
			"FROM ("+
			"SELECT objectname, json_array_elements(to_json(metadatadescription->'fields'))->>'fullName' AS fieldName, json_array_elements(to_json(metadatadescription->'fields')) fieldJSON FROM fieldinventory"+
			") q1"+
			"full outer join ("+
			"SELECT objectname, json_array_elements(to_json(partnerdescription->'fields'))->>'name' AS fieldName, json_array_elements(to_json(partnerdescription->'fields')) fieldJSON FROM fieldinventory"+ 
			") q2"+
			"on q1.objectname=q2.objectname AND q1.fieldName=q2.fieldName"+
			") q4 WHERE objectname=? order by 1,2";
	
	public static boolean connect(String user, String pwd, String url) {
		boolean successful = false;
		myConnection = null;
		Properties props = new Properties();
		props.setProperty("user",user);
		props.setProperty("password",pwd);
		try {
			myConnection = DriverManager.getConnection(url, props);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
			return false;
		}
		if (myConnection != null) {
			successful = true;
		}
		return successful;
	}
	
	public static void initializeProps(String propFilePath) {
		if (propFilePath != null && propFilePath.length() > 0) {
			dbProperties = Utils.initProps(propFilePath);
		}
	}
	
	public static void initializeDB (String propFilePath) {
		initializeProps(propFilePath);
		connect(dbProperties.getProperty("username"),
				dbProperties.getProperty("password"),
				dbProperties.getProperty("url"));
	}
	
	public static boolean doesTableExist (String tablename) {
		boolean retval = false;
		
		if (tableExistsCheckStatement == null) prepareStatements();
		
		try {
			tableExistsCheckStatement.setString(1, tablename);
			ResultSet rs = tableExistsCheckStatement.executeQuery();
			if (rs.next()) {
				String tableCheckVal = rs.getString(1);
                logger.debug("Table check returned: " + tableCheckVal);
                if (tableCheckVal != null && tableCheckVal.length() > 0) {
                	retval = true;
                }
            }
		} catch (SQLException e) {
			logger.error(e);
		}
		return retval;
	}
	
	public static boolean createTable (String tablename) {
		boolean retval = false;
		String myDDL = tableCreateStatementSQL.replace("$$1$$", tablename);
		
		try {
			tableCreateStatement = myConnection.createStatement();
			retval = tableCreateStatement.execute(myDDL);
		} catch (SQLException e) {
			logger.error(e);
		}
		return retval;
	}
	
	public static boolean createColumn (String tablename, String columndef) {
		boolean retval = false;
		String myDDL = columnCreateStatementSQL.replace("$$1$$", tablename).replace("$$2$$", columndef);
		
		if (columnCreateStatement == null) prepareStatements();
		
		try {
			columnCreateStatement = myConnection.createStatement();
			retval = columnCreateStatement.execute(myDDL);
		} catch (SQLException e) {
			logger.error(e);
		}
		return retval;
	}
	
	public static boolean createColumnIfNotExists(String tablename, String columndef) {
		
		// assume first word of columndef is the name
		
		String columnname = columndef.substring(0, columndef.indexOf(" "));
		
		if (!doesColumnExist(tablename, columnname)) {
			return createColumn(tablename, columndef);
		} else {
			return true;
		}
	}

	public static boolean doesColumnExist (String tablename, String columnname) {
		boolean retval = false;
		
		if (columnExistsCheckStatement == null) prepareStatements();
		
		try {
			columnExistsCheckStatement.setString(1, tablename.toLowerCase());
			columnExistsCheckStatement.setString(2, columnname.toLowerCase());
			ResultSet rs = columnExistsCheckStatement.executeQuery();
			if (rs.next()) {
				String tableCheckVal = rs.getString(1);
                logger.trace("Table check returned: " + tableCheckVal);
                if (tableCheckVal != null && tableCheckVal.length() > 0) {
                	retval = true;
                }
            }
		} catch (SQLException e) {
			logger.error(e);
		}
		return retval;
	}
	
	private static void prepareStatements() {
		try {
			tableExistsCheckStatement = myConnection.prepareStatement(tableCheckStatementSQL);
			columnExistsCheckStatement = myConnection.prepareStatement(columnCheckStatementSQL);
			insertObjectDataStatement = myConnection.prepareStatement(insertObjectDataSQL);
			deleteObjectDataStatement = myConnection.prepareStatement(deleteObjectDataSQL);
			getFieldDefinitionsStatement = myConnection.prepareStatement(getFieldDefinitionsSQL);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e);
		}
		
	}

	public static boolean storeObjectDataInDB(ObjectSFData od) {
		boolean retval = false;
		
		if (insertObjectDataStatement == null || deleteObjectDataStatement == null) prepareStatements();
		
		try {
			deleteObjectDataStatement.setString(1, od.getMyOrgId());
			deleteObjectDataStatement.setString(2, od.getName());
			deleteObjectDataStatement.execute();
			
			insertObjectDataStatement.setString(1, od.getMyOrgId());
			insertObjectDataStatement.setString(2, od.getName());
			
			PGobject metadataJsonObject = new PGobject();
			metadataJsonObject.setType("jsonb");
			metadataJsonObject.setValue(od.getMetadataAsJSONString());
			insertObjectDataStatement.setObject(3, metadataJsonObject);
			
			PGobject describeJsonObject = new PGobject();
			describeJsonObject.setType("jsonb");
			describeJsonObject.setValue(od.getDescribeAsJSONString());
			insertObjectDataStatement.setObject(4, describeJsonObject);
			
			return insertObjectDataStatement.execute();
		} catch (SQLException e) {
			logger.error(e);
		}
		return retval;
		
	}
	
}
