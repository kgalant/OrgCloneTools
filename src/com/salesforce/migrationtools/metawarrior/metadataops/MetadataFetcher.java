package com.salesforce.migrationtools.metawarrior.metadataops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import com.salesforce.migrationtools.MetadataLoginUtil;
import com.salesforce.migrationtools.metawarrior.MWProperties;
import com.salesforce.migrationtools.metawarrior.metadata.MetadataItem;
import com.salesforce.migrationtools.metawarrior.metadata.MetadataType;
import com.salesforce.migrationtools.metawarrior.metadata.MetadataTypes;
import com.sforce.soap.metadata.DescribeMetadataObject;
import com.sforce.soap.metadata.DescribeMetadataResult;
import com.sforce.soap.metadata.FileProperties;
import com.sforce.soap.metadata.ListMetadataQuery;
import com.sforce.soap.metadata.MetadataConnection;
import com.sforce.ws.ConnectionException;

public class MetadataFetcher {
	private MetadataConnection myConnection;
	private double apiVersion;
	private String myFriendlyName;
	private MWProperties props;
	
	private static Logger logger;
	
	public MetadataFetcher(String friendlyName, MWProperties mwProperties, Logger l) {
		logger = l;
		props = mwProperties;
		myFriendlyName = friendlyName;
		apiVersion = Double.parseDouble(props.getMWProperty(MWConstants.ORGPROPERTIESAPIVERSION));
	}
	
	private MetadataFetcher() {}
	
	
	public boolean hasValidConnection() {
		
		// TODO: figure out how to check for real - SessionID can be stale
		
		if (myConnection.getSessionHeader().getSessionId() != null) {
			return true;
		}
		return false;
	}
	
	public boolean connect() throws ConnectionException {
		
		
		
		String username = props.getOrgProperty(myFriendlyName,MWConstants.ORGPROP_USERNAME);
		String pwd = props.getOrgProperty(myFriendlyName,MWConstants.ORGPROP_PWD);
		String serverUrl = props.getOrgProperty(myFriendlyName,MWConstants.ORGPROP_SERVERURL);
		
		logger.debug("Asked to login to " + username + " : " + serverUrl);
		
		myConnection = MetadataLoginUtil.mdLogin(serverUrl + props.getMWProperty(MWConstants.ORGPROPERTIESURLBASE) + props.getMWProperty(MWConstants.ORGPROPERTIESAPIVERSION), username, pwd);
		
		return myConnection != null;
	}
	
	public MetadataTypes getMetadataTypes() throws NumberFormatException, ConnectionException {
		MetadataTypes types = new MetadataTypes();
		
		logger.debug("Asked to login to " + myFriendlyName);
		
		// check if we have a restricted list of types to deal with
		
		HashSet<String> restrictedProps = null; 
		
		if (props.getOrgProperty(myFriendlyName, MWConstants.ORGPROP_MDTYPES) != null) {
			restrictedProps = new HashSet<String>();
			String[] restrictedMdTypes = props.getOrgProperty(myFriendlyName,MWConstants.ORGPROP_MDTYPES).split(",");
			restrictedProps.addAll(new ArrayList<String>(Arrays.asList(restrictedMdTypes)));
		}
		
		
		DescribeMetadataResult dmr = myConnection.describeMetadata(apiVersion);
		for (DescribeMetadataObject obj : dmr.getMetadataObjects()) {
			MetadataType type = new MetadataType(obj);
			if ((restrictedProps != null && restrictedProps.contains(type.getTypeName())) || restrictedProps == null) {
				type.setItemCount(fetchItemCount(type.getTypeName()));
				types.addType(type);
			}
		}
		return types;
	}
	
	public int fetchItemCount (String metadataType) throws ConnectionException {
		
		logger.debug("Fetching item count for " + metadataType + " in " + myFriendlyName);
		
		ListMetadataQuery query = new ListMetadataQuery();
		query.setType(metadataType);
		
		// TODO: add folder handling
		
		/*String folderName = null;
		if (isFolder) {
			folderProperties = folder.next(); 
			folderName = folderProperties.getFullName();
			query.setFolder(folderName);
		}*/
		
		// Assuming that the SOAP binding has already been established.
		
		// generate full metadata inventory
		
		FileProperties[] srcMd = myConnection.listMetadata(new ListMetadataQuery[] { query }, apiVersion);
		
		/*
		 
			System.out.printf("%-80.80s","Processing folder: " + folderName + " ");
			// fetch folders themselves
			ArrayList<FileProperties> filenameList = new ArrayList<FileProperties>();
			filenameList.add(folderProperties);
			metadataMap.put(folderProperties.getFileName(), filenameList);
			itemCount++;
		}
		*/
		return srcMd.length;
	}
	
}
