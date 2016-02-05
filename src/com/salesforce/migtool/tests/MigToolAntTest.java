package com.salesforce.migtool.tests;

import java.io.File;
import java.util.Properties;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DefaultLogger;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.ProjectHelper;

import com.salesforce.migrationtools.Utils;


public class MigToolAntTest {

	/*
	 * param 1: name path of package.xml file
	 * param 2: name of output file
	 * param 3: properties file
	 */
	
	public static void main(String[] args) throws Exception {

		Properties props = Utils.initProps(args[2]);
		
		File buildFile = new File("resources/build.xml");
		Project p = new Project();
		p.setProperty("username", props.getProperty("username"));
		p.setProperty("password", props.getProperty("password"));
		p.setProperty("serverurl", props.getProperty("serverurl"));
		p.setProperty("maxPoll", props.getProperty("maxPoll"));
		p.setProperty("pollWaitMillis", props.getProperty("pollWaitMillis"));
		p.setProperty("dirname", args[1]);
		p.setProperty("packagefile", args[0]);
		p.setUserProperty("ant.file", buildFile.getAbsolutePath());		
		DefaultLogger consoleLogger = new DefaultLogger();
		consoleLogger.setErrorPrintStream(System.err);
		consoleLogger.setOutputPrintStream(System.out);
		consoleLogger.setMessageOutputLevel(Project.MSG_INFO);
		p.addBuildListener(consoleLogger);

		try {
			p.fireBuildStarted();
			p.init();
			ProjectHelper helper = ProjectHelper.getProjectHelper();
			p.addReference("ant.projectHelper", helper);
			helper.parse(p, buildFile);
			p.executeTarget("retrieveUnpackaged");
			p.fireBuildFinished(null);
		} catch (BuildException e) {
			p.fireBuildFinished(e);
		}
		

	}

}
