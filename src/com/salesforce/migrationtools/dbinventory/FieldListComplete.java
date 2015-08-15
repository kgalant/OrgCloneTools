package com.salesforce.migrationtools.dbinventory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.salesforce.migrationtools.MetadataLoginUtil;
import com.salesforce.migrationtools.ObjectSFData;
import com.salesforce.migrationtools.Utils;
import com.sforce.soap.metadata.FileProperties;
import com.sforce.soap.metadata.ListMetadataQuery;
import com.sforce.soap.metadata.MetadataConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.soap.metadata.*;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.GetUserInfoResult;
import com.sforce.soap.partner.PartnerConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FieldListComplete {

	private static String outputDir = "";

	private Properties sourceProps;
	private Properties fetchProps;

	private String srcUrl;
	private String srcUser;
	private String srcPwd;
	private static final double API_VERSION = 34.0;
	private static double myApiVersion;
	private static final String urlBase = "/services/Soap/u/";

	private static String orgId = null;
	private static String orgName = null;

	private boolean lookupfiles;
	private boolean nonlookupfiles;
	private boolean allfiles;

	private HashMap<String, Metadata> metadataMap = new HashMap<String, Metadata>();
	private HashMap<String, DescribeSObjectResult> describeMap = new HashMap<String, DescribeSObjectResult>();
	private HashMap<String, ObjectSFData> objectSFDataMap = new HashMap<String, ObjectSFData>();
	ArrayList<String> sortedNames = new ArrayList<String>();

	String authEndPoint = "";
	String user = "";
	String pwd = "";

	private MetadataConnection metadataConnection;
	private PartnerConnection partnerConnection;

	private static final Logger logger = LogManager.getLogger("MyLogger");

	public static void main(String[] args) throws ConnectionException {
		if (args.length < 2) {
			System.out.println("Usage parameters: <org property file path> <objectlist property path>");
			System.out
					.println("Example: c:\\temp\\migration\\test.properties c:\\temp\\migration\\fieldlist.properties - will output files for the objects specified to the path specified");
			System.out.println("Parameters not supplied - exiting.");
			System.exit(0);
		}

		FieldListComplete sample = new FieldListComplete();

		if (args.length > 0) {
			sample.sourceProps = Utils.initProps(args[0]);
		}

		if (args.length > 1) {
			sample.fetchProps = Utils.initProps(args[1]);
		}

		initializeDB();

		sample.run();
	}

	private static void initializeDB() {
		DBOps.initializeDB("c:\\temp\\fieldlist\\db.properties");
		DBOps.createTable("dbinventory");
		DBOps.createColumnIfNotExists("dbinventory", "orgid character varying(18)");
		DBOps.createColumnIfNotExists("dbinventory", "orgname character varying(255)");

		DBOps.createTable("fieldinventory");
		DBOps.createColumnIfNotExists("fieldinventory", "orgid character varying(18)");
		DBOps.createColumnIfNotExists("fieldinventory", "objectname character varying(255)");
		DBOps.createColumnIfNotExists("fieldinventory", "metadatadescription jsonb");
		DBOps.createColumnIfNotExists("fieldinventory", "partnerdescription jsonb");

		if (!DBOps.doesColumnExist("maintable", "somecolumn")) {
			DBOps.createColumn("maintable", "somecolumn character varying(200)");
		}
	}

	public void run() throws ConnectionException {

		String objectsToExamine = fetchProps.getProperty("objects") == null ? "" : fetchProps.getProperty("objects");

		myApiVersion = sourceProps.getProperty("apiversion") == null ? API_VERSION : Double.parseDouble(sourceProps.getProperty("apiversion"));
		srcUrl = sourceProps.getProperty("serverurl") + urlBase + myApiVersion;
		srcUser = sourceProps.getProperty("username");
		srcPwd = sourceProps.getProperty("password");
		lookupfiles = fetchProps.getProperty("lookupfiles").equals("1") ? true : false;
		nonlookupfiles = fetchProps.getProperty("nonlookupfiles").equals("1") ? true : false;
		allfiles = fetchProps.getProperty("allfiles").equals("1") ? true : false;

		outputDir = Utils.checkPathSlash(fetchProps.getProperty("targetdirectory"));

		// Make a login call

		this.metadataConnection = MetadataLoginUtil.mdLogin(srcUrl, srcUser, srcPwd);
		this.partnerConnection = MetadataLoginUtil.soapLogin(srcUrl, srcUser, srcPwd);

		getOrgParameters();

		readCustomObjects(objectsToExamine.split(","));

		computeNumberOfObjects();

		// outputObjects();

		populateObjectSFMap();

		// storeObjectDefinitionsInDB();

		createObjectTables();

	}

	private void populateObjectSFMap() {

		for (String objectName : sortedNames) {
			logger.debug("Processing: " + objectName);

			Metadata md = metadataMap.get(objectName);

			DescribeSObjectResult desc = describeMap.get(objectName);

			if (md == null) {
				logger.debug("Metadata doesn't contain info about " + objectName);
			}
			if (desc == null) {
				logger.debug("Describe doesn't contain info about " + objectName);
			}

			ObjectSFData od = new ObjectSFData(orgId, md, desc);

			objectSFDataMap.put(objectName, od);
		}
	}

	private void createObjectTables() {
		for (String objectName : sortedNames) {
			logger.debug("Creating: " + objectName);

			DBOps.createTable(objectName);

			// now get list of fields

			JsonNode root = objectSFDataMap.get(objectName).getMyJson();

			// System.out.println(root.toString());

			JsonNode fieldsArray = root.path("fields");

			if (fieldsArray.isArray()) {
				for (JsonNode fieldNode : fieldsArray) {
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
					}

				}
			}

		}

	}

	private void getOrgParameters() {
		// TODO Auto-generated method stub
		try {
			GetUserInfoResult result = partnerConnection.getUserInfo();

			orgId = result.getOrganizationId();
			orgName = result.getOrganizationName();

		} catch (ConnectionException e) {
			// TODO Auto-generated catch block
			logger.error(e);
		}

	}

	private void storeObjectDefinitionsInDB() {

		for (String objectName : sortedNames) {

			ObjectSFData od = objectSFDataMap.get(objectName);
			if (od != null) {
				od.storeInDB();
			}
			System.out.println(od.toJSON());

		}
	}

	private void computeNumberOfObjects() {
		HashSet<String> objectNames = new HashSet<String>();
		objectNames.addAll(metadataMap.keySet());
		objectNames.addAll(describeMap.keySet());

		logger.error("Total objects to examine: " + objectNames.size());
		sortedNames = new ArrayList<String>(objectNames);
		Collections.sort(sortedNames);
		logger.debug("Objects list: " + ArrayUtils.toString(sortedNames, ""));

	}

	public FieldListComplete() {

	}

	private void readCustomObjects(String[] objects) throws ConnectionException {

		ArrayList<String[]> myFetchChunks = new ArrayList<String[]>();
		ArrayList<String> objectNamesMD = new ArrayList<String>();
		ArrayList<String> objectNamesPartner = new ArrayList<String>();

		// check if we have to get a list of objects first

		if (objects.length == 1 && objects[0].equals("*")) {
			// do MdAPI check for objects

			ListMetadataQuery query = new ListMetadataQuery();
			query.setType("CustomObject");

			FileProperties[] srcMd = this.metadataConnection.listMetadata(new ListMetadataQuery[] { query }, myApiVersion);
			if (srcMd != null && srcMd.length > 0) {
				for (FileProperties n : srcMd) {
					if (n.getFullName().matches(".*__.*__c")) {
						// System.out.println(obj.getFullName() +
						// " is part of a managed package, skipping...");
						continue;
					}
					objectNamesMD.add(n.getFullName());
				}
			}

			// do PartnerAPI check for objects

			// Make the describeGlobal() call

			DescribeGlobalResult dgr = partnerConnection.describeGlobal();

			// Get the sObjects from the describe global result

			DescribeGlobalSObjectResult[] sObjResults = dgr.getSobjects();

			for (DescribeGlobalSObjectResult res : sObjResults) {
				objectNamesPartner.add(res.getName());
			}

		}

		String[] objectsToProcess = new String[objectNamesMD.size() > 0 ? objectNamesMD.size() : objects.length];

		logger.error("Metadata API found " + objectsToProcess.length + " objects to process.");
		logger.error("Partner API found " + objectNamesPartner.size() + " objects to process.");

		objectsToProcess = objectNamesMD.toArray(objectsToProcess);

		logger.error("MdAPI Objects to process: " + objectsToProcess.length);

		// split into 10-item pieces (readMetadata limit)

		int chunkSize = 10;

		if (objectsToProcess.length < chunkSize) {
			myFetchChunks.add(objects);
		} else {
			int counter = 0;
			do {
				int from = 0 + (counter * chunkSize);
				int to = (counter + 1) * chunkSize;
				if (to > objectsToProcess.length) {
					to = objectsToProcess.length;
				}
				myFetchChunks.add(Arrays.copyOfRange(objectsToProcess, from, to));
			} while (objectsToProcess.length > (chunkSize * ++counter));
		}

		ArrayList<String> fieldStringsLookupAll = new ArrayList<String>();

		int objectsProcessed = 0;

		for (String[] chunk : myFetchChunks) {

			logger.error("Looking at: " + ArrayUtils.toString(chunk, ""));
			try {
				ReadResult readResult = metadataConnection.readMetadata("CustomObject", chunk);
				Metadata[] mdInfo = readResult.getRecords();
				logger.debug("Number of component info returned: " + mdInfo.length);
				for (Metadata md : mdInfo) {
					if (md != null) {
						metadataMap.put(md.getFullName(), md);
						objectsProcessed++;
					} else {
						logger.debug("Empty metadata.");
					}
				}
			} catch (ConnectionException ce) {
				logger.debug("Chunk failed: " + ArrayUtils.toString(chunk, ""));
				ce.printStackTrace();
				logger.debug("Will try individual pieces.");
				for (String chunkPiece : chunk) {
					String[] smallChunk = new String[1];
					smallChunk[0] = chunkPiece;
					try {
						ReadResult readResult = metadataConnection.readMetadata("CustomObject", smallChunk);
						Metadata[] mdInfo = readResult.getRecords();
						// System.out.println("Number of component info returned: "
						// + mdInfo.length);
						for (Metadata md : mdInfo) {
							if (md != null) {
								metadataMap.put(md.getFullName(), md);
								objectsProcessed++;
							} else {
								System.out.println("Empty metadata.");
							}
						}
					} catch (ConnectionException ce2) {
						logger.debug("Chunk failed: " + ArrayUtils.toString(smallChunk, ""));
						ce2.printStackTrace();
					}
				}
			} finally {

			}

		}

		logger.error("Objects processed: " + objectsProcessed);

		// now chunk the describeobjects

		chunkSize = 100;
		myFetchChunks.clear();
		objectsProcessed = 0;

		objectsToProcess = new String[objectNamesPartner.size() > 0 ? objectNamesPartner.size() : objects.length];
		objectsToProcess = objectNamesPartner.toArray(objectsToProcess);

		logger.error("SoapAPI Objects to process: " + objectsToProcess.length);

		if (objectsToProcess.length < chunkSize) {
			myFetchChunks.add(objects);
		} else {
			int counter = 0;
			do {
				int from = 0 + (counter * chunkSize);
				int to = (counter + 1) * chunkSize;
				if (to > objectsToProcess.length) {
					to = objectsToProcess.length;
				}
				myFetchChunks.add(Arrays.copyOfRange(objectsToProcess, from, to));
			} while (objectsToProcess.length > (chunkSize * ++counter));
		}

		// now go get the describe results
		for (String[] chunk : myFetchChunks) {
			try {
				logger.error("Looking at: " + ArrayUtils.toString(chunk, ""));
				DescribeSObjectResult[] describeSObjectResults = partnerConnection.describeSObjects(chunk);
				logger.debug("Number of component info returned: " + describeSObjectResults.length);
				for (DescribeSObjectResult dr : describeSObjectResults) {
					if (dr != null) {
						describeMap.put(dr.getName(), dr);
						objectsProcessed++;
					} else {
						System.out.println("Empty metadata.");
					}
				}
			} catch (ConnectionException ce) {
			}
		}

		logger.error("Objects processed: " + objectsProcessed);

	}
}
