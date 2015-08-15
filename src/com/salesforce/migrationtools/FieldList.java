package com.salesforce.migrationtools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;

import com.sforce.soap.metadata.FileProperties;
import com.sforce.soap.metadata.ListMetadataQuery;
import com.sforce.soap.metadata.MetadataConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.soap.metadata.*;

public class FieldList {

	private static String outputDir = "";

	private Properties sourceProps;
	private Properties fetchProps;

	private String srcUrl;
	private String srcUser;
	private String srcPwd;
	private static final double API_VERSION = 33.0;
	private static double myApiVersion;
	private static final String urlBase = "/services/Soap/u/";
	
	private boolean lookupfiles;
	private boolean nonlookupfiles;
	private boolean allfiles;

	String authEndPoint = "";
	String user = "";
	String pwd = "";

	private MetadataConnection metadataConnection;

	public static void main(String[] args) throws ConnectionException {
		if (args.length < 2) {
			System.out.println("Usage parameters: <org property file path> <objectlist property path>");
			System.out
					.println("Example: c:\\temp\\migration\\test.properties c:\\temp\\migration\\fieldlist.properties - will output files for the objects specified to the path specified");
			System.out.println("Parameters not supplied - exiting.");
			System.exit(0);
		}

		FieldList sample = new FieldList();

		if (args.length > 0) {
			sample.sourceProps = Utils.initProps(args[0]);
		}

		if (args.length > 1) {
			sample.fetchProps = Utils.initProps(args[1]);
		}

		sample.run();
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

		readCustomObjectFields(objectsToExamine.split(","));

	}

	public FieldList() {

	}

	private void readCustomObjectFields(String[] objects) throws ConnectionException {

		ArrayList<String[]> myFetchChunks = new ArrayList<String[]>();
		ArrayList<String> objectNames = new ArrayList<String>();

		// check if we have to get a list of objects first


		if (objects.length == 1 && objects[0].equals("*")) {
			ListMetadataQuery query = new ListMetadataQuery();
			query.setType("CustomObject");

			FileProperties[] srcMd = this.metadataConnection.listMetadata(new ListMetadataQuery[] { query }, myApiVersion);
			if (srcMd != null && srcMd.length > 0) {
				for (FileProperties n : srcMd) {
					if (n.getFullName().matches(".*__.*__c")) {
//						System.out.println(obj.getFullName() + " is part of a managed package, skipping...");
						continue;
					}
					objectNames.add(n.getFullName());
				}
			}
		}

		String[] objectsToProcess = new String[objectNames.size() > 0 ? objectNames.size() : objects.length];

		objectsToProcess = objectNames.toArray(objectsToProcess);

		System.out.println("Objects to process: " + objectsToProcess.length);
		
		// split into 10-item pieces (readMetadata limit)
		
		int chunkSize = 10;

		if (objectsToProcess.length < chunkSize) {
			myFetchChunks.add(objects);
		} else {
			int counter = 0;
			do {
				int from = 0 + (counter * chunkSize);
				int to = (counter + 1) * chunkSize;
				myFetchChunks.add(Arrays.copyOfRange(objectsToProcess, from, to));
			} while (objectsToProcess.length > (chunkSize * ++counter));
		}

		ArrayList<String> fieldStringsLookupAll = new ArrayList<String>();
		
		int objectsProcessed = 0;
		
		for (String[] chunk : myFetchChunks) {
			
			System.out.println("Looking at: " + ArrayUtils.toString(chunk, ""));
			try {
				ReadResult readResult = metadataConnection.readMetadata("CustomObject", chunk);
				Metadata[] mdInfo = readResult.getRecords();
				System.out.println("Number of component info returned: " + mdInfo.length);
				for (Metadata md : mdInfo) {
					if (md != null) {
						fieldStringsLookupAll.addAll(processObject(md));
						objectsProcessed++;
					} else {
						System.out.println("Empty metadata.");
					}
				}
			} catch (ConnectionException ce) {
				System.out.println("Chunk failed: " + ArrayUtils.toString(chunk, ""));
				ce.printStackTrace();
				System.out.println("Will try individual pieces.");
				for (String chunkPiece : chunk) {
					String[] smallChunk = new String[1];
					smallChunk[0] = chunkPiece;
					try {
						ReadResult readResult = metadataConnection.readMetadata("CustomObject", smallChunk);
						Metadata[] mdInfo = readResult.getRecords();
//						System.out.println("Number of component info returned: " + mdInfo.length);
						for (Metadata md : mdInfo) {
							if (md != null) {
								fieldStringsLookupAll.addAll(processObject(md));
								objectsProcessed++;
							} else {
								System.out.println("Empty metadata.");
							}
						}
					} catch (ConnectionException ce2) {
						System.out.println("Chunk failed: " + ArrayUtils.toString(smallChunk, ""));
						ce2.printStackTrace();
					}
				}
			} finally {
				
			}
			
		}
		
		System.out.println("Objects processed: " + objectsProcessed);
		
		// now output the full list
		Collections.sort(fieldStringsLookupAll);
		Utils.writeFile(outputDir +"all_lookups.txt", fieldStringsLookupAll);
	}
	
	private ArrayList<String> processObject (Metadata md) {
		
		ArrayList<String> retval = new ArrayList<String>();
		
		CustomObject obj = (CustomObject) md;
		System.out.println("Custom object full name: " + obj.getFullName());
//		System.out.println("Label: " + obj.getLabel());
//		System.out.println("Number of custom fields: " + obj.getFields().length);
		
		// check if managed package field, if so, skip
		
		if (obj.getFullName().matches(".*__.*__c")) {
			System.out.println(obj.getFullName() + " is part of a managed package, skipping...");
			return retval;
		}
		
		CustomField[] fields = obj.getFields();

		ArrayList<String> fieldStringsNonLookup = new ArrayList<String>();
		ArrayList<String> fieldStringsLookup = new ArrayList<String>();

		for (CustomField field : fields) {
			if (field != null && field.getType() != null) {
				if (field.getFormula() == null && !(field.getType().name().toLowerCase().equals("summary"))
						&& (field.getType().name().toLowerCase().equals("lookup") ||
							field.getType().name().toLowerCase().equals("masterdetail"))	
								) {
					fieldStringsLookup.add(obj.getFullName() + "." + field.getFullName() + " --> " + field.getReferenceTo() + 
							(field.getType().name().toLowerCase().equals("masterdetail") ? " (MD)," : ","));
				}
				if (field.getFormula() == null && !(field.getType().name().toLowerCase().equals("summary"))) {
					fieldStringsNonLookup.add(obj.getFullName() + "." + field.getFullName() + ",");
				}
			}
		}

		Utils.checkDir(outputDir);

		Collections.sort(fieldStringsLookup);
		Collections.sort(fieldStringsNonLookup);
		
		if (lookupfiles) {
			Utils.writeFile(outputDir + obj.getFullName() + "_lookups.txt", fieldStringsLookup);
		}
		retval.addAll(fieldStringsLookup);
		if (nonlookupfiles) {
			Utils.writeFile(outputDir + obj.getFullName() + "nonlookups.txt", fieldStringsNonLookup);
		}
		fieldStringsLookup.addAll(fieldStringsNonLookup);
		Collections.sort(fieldStringsLookup);
		if (allfiles) {
			Utils.writeFile(outputDir + obj.getFullName() + "_all.txt", fieldStringsLookup);
		}
		return retval;
	}

}
