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
	private Properties dbProps;

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
	private HashMap<String, ArrayList<String>> objectQueryListMap = new HashMap<String, ArrayList<String>>();
	
	ArrayList<String> sortedNames = new ArrayList<String>();

	String authEndPoint = "";
	String user = "";
	String pwd = "";

	private MetadataConnection metadataConnection;
	private PartnerConnection partnerConnection;

	private static final Logger logger = LogManager.getLogger("MyLogger");

	public static void main(String[] args) throws ConnectionException {
		if (args.length < 3) {
			System.out.println("Usage parameters: <org property file path> <objectlist property path> <database config file path>");
			System.out
					.println("Example: c:\\temp\\migration\\test.properties c:\\temp\\migration\\fieldlist.properties c:\\temp\\migration\\db.properties - will output files for the objects specified to the path specified");
			System.out.println("Parameters not supplied - exiting.");
			System.exit(0);
		}

		FieldListComplete sample = new FieldListComplete();

		if (args.length > 2) {
			sample.sourceProps = Utils.initProps(args[0]);
			sample.fetchProps = Utils.initProps(args[1]);
			sample.dbProps = Utils.initProps(args[2]);
		}

		sample.initializeDB();

		sample.run();
	}

	private void initializeDB() {
		DBOps.initializeDB(dbProps);
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
		
		generateObjectQueries();

	}

	/*
	 * This method will generate the queries needed to extract data from the SF database
	 */
	
	private void generateObjectQueries() {
		
		HashMap<String, ArrayList<String>> queryList = new HashMap<String, ArrayList<String>>();
		
		for (String objectName : sortedNames) {
			queryList.put(objectName, generateObjectQuery(objectName));
		}		
	}
	
	/*
	 * This method generates a select all fields query based on the object name and the database field inventory
	 */
	

	private ArrayList<String> generateObjectQuery(String objectName) {
		final String selectPart = "SELECT Id";
		final String fromPart = " FROM " + objectName;
		int queryLengthLimit = 10000;
		ArrayList<String> generatedQueries = new ArrayList<String>();
		
		ArrayList<String> fieldNames = DBOps.getFieldNamesForObject(orgId, objectName);
		
		StringBuilder query = new StringBuilder("");
		query.append(selectPart);
		
		for (String fieldName : fieldNames) {

			// Skip if the field is Id, we'll add that elsewhere
			if (fieldName.equalsIgnoreCase("id"))
				continue;
			
			String fieldPart = "," + fieldName;
			// check if adding this field would take us over the character limit
			if (query.length() + fieldPart.length() + fromPart.length() > queryLengthLimit) {
				// finish this query, start a new one
				query.append(fromPart);
				generatedQueries.add(query.toString());
				query = new StringBuilder("");
				query.append(selectPart); 
			} else {
//				just add this field to the query
				query.append(fieldPart);
			}			
		}
		
		return generatedQueries;
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

			JsonNode root = objectSFDataMap.get(objectName).getMyJson();
			
			DBOps.createTable(orgId, objectName, root);		
		}
	}
	
	
	
	

	/* 
	 * This method fetches the name of the org so that it can be stored/updated in the database
	 */
	
	private void getOrgParameters() {
		
		try {
			GetUserInfoResult result = partnerConnection.getUserInfo();

			orgId = result.getOrganizationId();
			orgName = result.getOrganizationName();

		} catch (ConnectionException e) {
			// TODO Auto-generated catch block
			logger.error(e);
		}

	}
	
	/*
	 * This method stores all the fetched object definitions in the database
	 */

	private void storeObjectDefinitionsInDB() {

		for (String objectName : sortedNames) {

			ObjectSFData od = objectSFDataMap.get(objectName);
			if (od != null) {
				od.storeInDB();
			}
//			System.out.println(od.toJSON());

		}
	}
	
	/*
	 * This method computes the number of objects that are available through the APIs
	 */

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

	/*
	 * This method reads data for a given set of objects from the database
	 */
	
	private void readCustomObjects(String[] objects) throws ConnectionException {

		ArrayList<String[]> myFetchChunks;
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
		
//		now that we know what's there, lets process the MdAPI objects

		String[] objectsToProcess = new String[objectNamesMD.size() > 0 ? objectNamesMD.size() : objects.length];

		logger.error("Metadata API found " + objectsToProcess.length + " objects to process.");
		logger.error("Partner API found " + objectNamesPartner.size() + " objects to process.");

		objectsToProcess = objectNamesMD.size() > 0 ? objectNamesMD.toArray(objectsToProcess) :objects ;

		logger.error("MdAPI Objects to process: " + objectsToProcess.length);

		// split into 10-item pieces (readMetadata limit)

		int chunkSize = 10;

		myFetchChunks = getChunkList(objectsToProcess, chunkSize);

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
		objectsProcessed = 0;

		objectsToProcess = new String[objectNamesPartner.size() > 0 ? objectNamesPartner.size() : objects.length];
		objectsToProcess = objectNamesPartner.size() > 0 ? objectNamesPartner.toArray(objectsToProcess) :objects ;

		logger.error("SoapAPI Objects to process: " + objectsToProcess.length);

		myFetchChunks = getChunkList(objectsToProcess, chunkSize);

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
	
	private ArrayList<String[]> getChunkList (String[] objectsToProcess, int chunkSize) {
		ArrayList<String[]> myFetchChunks = new ArrayList<String[]>();
		
		if (objectsToProcess.length < chunkSize) {
			myFetchChunks.add(objectsToProcess);
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
		
		return myFetchChunks;
	}
}
