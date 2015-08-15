package com.salesforce.migrationtools.metawarrior;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.salesforce.migrationtools.Utils;

public class OrgPropertyInitializer {
	
	private ArrayList<Properties> orgPropertiesList;
	private String orgPropertiesFilesLocation;
	
	private static final Logger logger = LogManager.getLogger();
	
	public OrgPropertyInitializer(String orgPropertiesFilesLocation, ArrayList<Properties> orgPropertiesList) {
		this.orgPropertiesFilesLocation = orgPropertiesFilesLocation;
		this.orgPropertiesList = orgPropertiesList;
	}

	/*
	 * This method will look in the directory provided and read/parse all the properties files in it
	 */
	
	public void initializeOrgProperties() {
		for (File f : FileUtils.listFiles(new File(orgPropertiesFilesLocation), TrueFileFilter.INSTANCE, FalseFileFilter.INSTANCE)) {
			Properties p = Utils.initProps(f);
			orgPropertiesList.add(p);
			logger.info("Loaded org properties file. " + f.getName());
		}
	}

}
