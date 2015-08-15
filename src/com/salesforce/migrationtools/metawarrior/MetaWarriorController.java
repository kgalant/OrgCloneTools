package com.salesforce.migrationtools.metawarrior;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.salesforce.migrationtools.metawarrior.metadata.MetadataTypes;
import com.salesforce.migrationtools.metawarrior.metadataops.MWConstants;
import com.salesforce.migrationtools.metawarrior.metadataops.MetadataFetcher;
import com.sforce.ws.ConnectionException;

public class MetaWarriorController {
	private static HashMap<String,MetadataTypes> metadataTypesHash = new HashMap<String,MetadataTypes>();
	private static HashMap<String,MetadataFetcher> metadataFetchersHash = new HashMap<String,MetadataFetcher>();
	
	private static MWProperties props;
	
	private static final Logger logger = LogManager.getLogger();
	
	public MetaWarriorController (MWProperties mwprops) {
		props = mwprops;
	}
	
	public String getMouseOver(String orgName) {
		String mouseover = "not connected";
		
		if (!metadataFetchersHash.containsKey(orgName)) {
			initFetcher(orgName);
		}
		
		if (metadataFetchersHash.get(orgName).hasValidConnection()) {
			return props.getOrgProperty(orgName, MWConstants.ORGPROP_USERNAME) + " : " + 
					props.getOrgProperty(orgName, MWConstants.ORGPROP_SERVERURL);	
		}
		
		return mouseover;
	}
	
	private void initFetcher (String orgName) {
		// check if we have a fetcher

		if (!metadataFetchersHash.containsKey(orgName)) {
			// initialize org
			MetadataFetcher mf = new MetadataFetcher(orgName,props, logger);
			
			metadataFetchersHash.put(orgName, mf);
			try {
				if (mf.connect() && mf.hasValidConnection()) {
					metadataTypesHash.put(orgName, mf.getMetadataTypes());
				}
			} catch (ConnectionException e) {
				// TODO Auto-generated catch block
				logger.debug("Could not initialize connection to org " + orgName, e);
			}
			
		}
	}

	public MetadataTypes getMDTypesHash(String orgName) {
		if (!metadataFetchersHash.containsKey(orgName)) {
			initFetcher(orgName);
		}
		
		return metadataTypesHash.get(orgName);
	}
}
