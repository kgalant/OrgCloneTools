package com.salesforce.migrationtools;

import java.util.Properties;

import com.sforce.ws.ConnectionException;

public class MigToolTest {

	/*
	 * param 1: name path of package.xml file
	 * param 2: name of output file
	 * param 3: properties file
	 */
	
	public static void main(String[] args) throws Exception {

		Properties props = Utils.initProps(args[2]);
		
		MetadataOps mdOps = new MetadataOps(props);
		
		mdOps.retrieveZip(args[0], args[1]);
		

	}

}
