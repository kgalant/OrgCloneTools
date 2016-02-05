package com.salesforce.migtool.tests;

import java.util.Properties;

import com.salesforce.migrationtools.Utils;
import com.salesforce.migtool.MetadataOps;
import com.sforce.ws.ConnectionException;

public class MigToolTestDeploy {

	/*
	 * param 1: name path of input zip file
	 * param 2: properties file
	 */
	
	public static void main(String[] args) throws Exception {

		Properties props = Utils.initProps(args[1]);
		
		MetadataOps mdOps = new MetadataOps(props);
		
		mdOps.deployZip(args[0]);
		

	}

}
