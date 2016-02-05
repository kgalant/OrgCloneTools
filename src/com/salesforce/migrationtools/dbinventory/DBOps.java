package com.salesforce.migrationtools.dbinventory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import com.fasterxml.jackson.databind.JsonNode;
import com.salesforce.migrationtools.ObjectSFData;
import com.salesforce.migrationtools.Utils;

public class DBOps {
	private static Properties dbProperties;
	
	private static Connection myConnection;
	
	private static PreparedStatement tableExistsCheckStatement = null;
	private static PreparedStatement columnExistsCheckStatement = null;
	private static PreparedStatement insertObjectDataStatement = null;
	private static PreparedStatement deleteObjectDataStatement = null;
	private static PreparedStatement insertFieldDataStatement = null;
	private static PreparedStatement deleteFieldDataStatement = null;
	private static PreparedStatement getFieldDefinitionsStatement = null;
	private static PreparedStatement selectFieldDataStatement = null;
	
	private static Statement columnCreateStatement = null; 
	private static Statement tableCreateStatement = null;

	 
	
	
	private static final Logger logger = LogManager.getLogger("MyLogger");
	
	private static final String tableCheckStatementSQL = "SELECT to_regclass(?)";
	private static final String columnCheckStatementSQL = "SELECT column_name FROM information_schema.columns WHERE table_name=? and column_name=?";
	private static final String columnCreateStatementSQL = "ALTER TABLE $$1$$ ADD COLUMN $$2$$";
	private static final String tableCreateStatementSQL = "CREATE TABLE IF NOT EXISTS $$1$$ ( ) WITH (OIDS=FALSE)";
	private static final String insertObjectDataSQL = "INSERT INTO fieldinventory (orgid, objectname, metadatadescription, partnerdescription) VALUES (?,?,?,?)";
	private static final String deleteObjectDataSQL = "DELETE FROM fieldinventory WHERE orgid=? AND objectname=?";
	private static final String insertFieldDataSQL = "INSERT INTO fieldlist (orgid, objectname, fieldname, fielddata) VALUES (?,?,?,?)";
	private static final String deleteFieldDataSQL = "DELETE FROM fieldlist WHERE orgid=? AND objectname=? and fieldname=?";
	private static final String selectFieldDataSQL = "SELECT fieldname FROM fieldlist WHERE orgid=? and objectname=?";
	
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
	
	public static void initializeDB (Properties dbProps) {
		dbProperties = dbProps;
		connect(dbProperties.getProperty("username"),
				dbProperties.getProperty("password"),
				dbProperties.getProperty("url"));
		
		DBOps.createTableInDB("dbinventory");
		DBOps.createColumnIfNotExists("dbinventory", "orgid character varying(18)");
		DBOps.createColumnIfNotExists("dbinventory", "orgname character varying(255)");

		DBOps.createTableInDB("fieldinventory");
		DBOps.createColumnIfNotExists("fieldinventory", "orgid character varying(18)");
		DBOps.createColumnIfNotExists("fieldinventory", "objectname character varying(255)");
		DBOps.createColumnIfNotExists("fieldinventory", "metadatadescription jsonb");
		DBOps.createColumnIfNotExists("fieldinventory", "partnerdescription jsonb");
		
		DBOps.createTableInDB("fieldlist");
		DBOps.createColumnIfNotExists("fieldlist", "orgid character varying(18)");
		DBOps.createColumnIfNotExists("fieldlist", "objectname character varying(255)");
		DBOps.createColumnIfNotExists("fieldlist", "fieldname character varying(255)");
		DBOps.createColumnIfNotExists("fieldlist", "fielddata jsonb");
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
	
	public static boolean createTableInDB (String tablename) {
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
			selectFieldDataStatement  = myConnection.prepareStatement(selectFieldDataSQL);
			insertFieldDataStatement = myConnection.prepareStatement(insertFieldDataSQL);
			deleteFieldDataStatement = myConnection.prepareStatement(deleteFieldDataSQL);
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
	
	public static boolean deleteFieldListValue(String orgId, String objectName, String fieldName) {
		boolean retval = false;
		
		if (insertFieldDataStatement == null || deleteFieldDataStatement == null) prepareStatements();
		
		try {
			deleteFieldDataStatement.setString(1, orgId);
			deleteFieldDataStatement.setString(2, objectName);
			deleteFieldDataStatement.setString(3, fieldName);
			return deleteFieldDataStatement.execute();
		} catch (SQLException e) {
			logger.error(e);
		}
		return retval;
		
	}

	public static boolean insertFieldListValue(String orgId, String objectName, String fieldName, JsonNode fieldNode) {
		boolean retval = false;
		
		if (insertFieldDataStatement == null || deleteFieldDataStatement == null) prepareStatements();
		
		try {
			insertFieldDataStatement.setString(1, orgId);
			insertFieldDataStatement.setString(2, objectName);
			insertFieldDataStatement.setString(3, fieldName);
			
			PGobject describeJsonObject = new PGobject();
			describeJsonObject.setType("jsonb");
			describeJsonObject.setValue(fieldNode.toString());
			insertFieldDataStatement.setObject(4, describeJsonObject);
			
			return insertFieldDataStatement.execute();
		} catch (SQLException e) {
			logger.error(e);
		}
		return retval;
		
	}
	
	/*
	 * This method will create a single table based on a tablename and its JSON representation
	 */
	
	public static void createTable(String orgId, String objectName, JsonNode root) {
		
		DBOps.createTableInDB(objectName);
		
		JsonNode fieldsArray = root.path("fields");

		if (fieldsArray.isArray()) {
			for (JsonNode fieldNode : fieldsArray) {
				createFieldInDB(orgId, objectName, fieldNode);
			}
		}
	}

	private static void createFieldInDB(String orgId, String objectName, JsonNode fieldNode) {
		// get name of field from JSON
		
		String fieldName = fieldNode.path("fullName").textValue();
		if (fieldName == null || fieldName.length() == 0 || fieldName.equals("null")) {
			fieldName = fieldNode.path("name").textValue();
		}
		String fieldDatatype = fieldNode.path("type").textValue();
		int fieldLength = fieldNode.path("length").intValue();
		int fieldScale = fieldNode.path("scale").intValue();
		logger.debug("Processing field: " + fieldName + " type: " + fieldDatatype + " length: " + fieldLength + " scale: " + fieldScale);

		boolean canHandleDatatype = false;
		String columndef = "";
		if (fieldDatatype != null) {
			switch (fieldDatatype.toLowerCase()) {
			case "string":
			case "text":
			case "phone":
			case "url":
			case "multipicklist":
			case "combobox":
				columndef = fieldName + " character varying (" + (fieldLength == 0 ? 255 : fieldLength) + ")";
				canHandleDatatype = true;
				break;
			case "date":
				columndef = fieldName + " date";
				canHandleDatatype = true;
				break;
			case "datetime":
				columndef = fieldName + " timestamp";
				canHandleDatatype = true;
				break;
			case "lookup":
			case "id":
			case "reference":
				columndef = fieldName + " character varying (18)";
				canHandleDatatype = true;
				break;
			case "currency":
			case "percent":
			case "_double":
			case "_int":
				columndef = fieldName + " numeric (18," + fieldScale + ")";
				canHandleDatatype = true;
				break;
			case "checkbox":
			case "_boolean":
				columndef = fieldName + " boolean";
				canHandleDatatype = true;
				break;
			case "longtextarea":
				columndef = fieldName + " character varying (" + fieldLength + ")";
				canHandleDatatype = true;
				break;
			case "picklist":
			case "email":
			case "textarea":
			case "address":
				columndef = fieldName + " character varying (" + (fieldLength != 0 ? fieldLength : 255) + ")";
				canHandleDatatype = true;
				break;
			case "summary":
			case "number":
				columndef = fieldName + " numeric (18," + (fieldScale != 0 ? fieldLength : 8) + ")";
				canHandleDatatype = true;
				break;
			default:

			}
			if (!DBOps.doesColumnExist(objectName, fieldName)) {
				if (canHandleDatatype) {
					DBOps.createColumn(objectName, columndef);
					logger.error("Table " + objectName + ", field " + columndef + " created.");
				} else {
					logger.error("Cannot handle field " + fieldName + " of data type: " + fieldDatatype);
				}
			} else {
				logger.error("Table " + objectName + ", field " + columndef + " already exists.");
			}
			
			// put it in the inventory table as well
			
			DBOps.deleteFieldListValue(orgId, objectName, fieldName);
			DBOps.insertFieldListValue(orgId, objectName, fieldName, fieldNode);
		}
	}
	
	public static ArrayList<String> getFieldNamesForObject(String orgId, String objectName) {
		ArrayList<String> retval = new ArrayList<String>();
		
		if (selectFieldDataStatement == null) prepareStatements();		
		
		try {
			selectFieldDataStatement.setString(1, orgId);
			selectFieldDataStatement.setString(2, objectName);
			
			ResultSet rs = selectFieldDataStatement.executeQuery();
			
			
			while (rs.next()) {
				retval.add(rs.getString(1));
			}
		} catch (SQLException e) {
			logger.error(e);
		}
		
		return retval;
	}
}
