package com.salesforce.migrationtools.metawarrior;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.salesforce.migrationtools.Utils;
import com.salesforce.migrationtools.metawarrior.metadataops.MWConstants;

public class MWProperties {
	private HashMap<String,Properties> orgPropertiesMap = new HashMap<String,Properties>();
	private Properties metawarriorProperties;
	
	private static final Logger logger = LogManager.getLogger();
	
	public MWProperties (String propertyPath) {
		metawarriorProperties = Utils.initProps(propertyPath);
		initializeOrgProperties();
	}
	
	public String getMWProperty (String propertyName) {
		return metawarriorProperties.getProperty(propertyName);
	}
	
	public String getOrgProperty(String orgFriendlyName, String propertyName) {
		if (orgPropertiesMap.containsKey(orgFriendlyName)) {
			return orgPropertiesMap.get(orgFriendlyName).getProperty(propertyName);
		} else {
			return null;
		}
	}
	
	public int getOrgsCount() {
		return orgPropertiesMap.size();
	}
	
	public Set<String> getOrgNames() {
		return orgPropertiesMap.keySet();
	}
	
	private void initializeOrgProperties() {
		for (File f : FileUtils.listFiles(new File(getMWProperty(MWConstants.ORGPROPERTIESDIRPARAM)), TrueFileFilter.INSTANCE, FalseFileFilter.INSTANCE)) {
			Properties p = Utils.initProps(f);
			if (orgPropertiesMap.containsKey(p.getProperty(MWConstants.ORGPROP_ORGNAME))) {
				logger.info("An org with friendly name " + p.getProperty(MWConstants.ORGPROP_ORGNAME) + " already defined. Will skip config file: " + f.getName());
			} else if (p.getProperty(MWConstants.ORGPROP_ORGNAME) == null) {
				logger.info("No friendly name defined in file: " + f.getName() + ", will skip this org.");
			} else {
				orgPropertiesMap.put(p.getProperty(MWConstants.ORGPROP_ORGNAME), p);
				logger.info("Loaded org properties file. " + f.getName());
			}
		}
	}
}
