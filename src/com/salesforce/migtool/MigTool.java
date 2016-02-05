/*
 * This is the main class of the migtool. It parses the commandline and generates appropriate actions on the MetadataOps
 * 
 * Also spits out the manpage if required
 */

package com.salesforce.migtool;

import java.io.File;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DefaultLogger;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.ProjectHelper;

import com.salesforce.migrationtools.Utils;

public class MigTool {

	// commandline stuff
	
	private static CommandLine line = null;
	private static Options options = new Options();
	private static OptionGroup commands = new OptionGroup();
	
	// connection options
	
	private static HashMap<String,String> connParams = new HashMap<String,String>();
	private static String[] managedConnParams = {"username", "password", "serverurl", "apiversion", "pollWaitMillis","maxPoll"};
	
	
	
	
	
	public static void main(String[] args) {
		
		setupOptions();
		setupConnParamDefaults();
		
		CommandLineParser parser = new DefaultParser();
	    try {
	        // parse the command line arguments
	        line = parser.parse( options, args );
	    }
	    catch( ParseException exp ) {
	        // oops, something went wrong
	        System.err.println( "Command line parsing failed.  Reason: " + exp.getMessage() );
	        System.exit(-1);
	    }
	    
	    // check if we have one of the core commands
	    
	    if (line != null) {
	    	if (!(line.hasOption("v") || line.hasOption("d") || line.hasOption("r"))) {
	    		
	    		// we don't, crash out
	    		
	    		System.out.println("Did not find primary command (v, d or r) on the command line. Please see help for syntax.");
	    		printHelp();
	    		System.exit(-1);
	    	} else {
	    		
	    		// we do, set up org params, crash out if we don't have enough
	    		
	    		setupOrgParameters();
	    		
	    		// now figure out what we're doing, move control there
	    		
	    		if (line.hasOption("v")) {
	    			executeDeploy(true);
	    		} else if (line.hasOption("d")) {
	    			executeDeploy(false);
	    		} else if (line.hasOption("r")) {
	    			executeRetrieve();
	    		}
	    		
	    		
	    	}
	    	
	    		
	    } else printHelp();

	}
	
	/*
	 * Do an ant retrieve
	 */
	
	private static void executeRetrieve() {
		
		// first, check needed parameters
		
		String[] cmdlineParams = line.getArgs();
		String outputDir = null;
		String packageFile = null;
		String outputFilename = "package.zip";
		
		
		// check if the second (first was org file) parameter exists (SF will check whether it's a valid package.xml)
		
		if (cmdlineParams.length >= 2) {
			packageFile = cmdlineParams[1];
			
			File checkFile = new File(packageFile);
			if (!checkFile.exists() || !checkFile.isFile()) {
				System.out.println("Package file provided (parameter #2, excluding any options): " + packageFile + " does not point to an existing file. Cannot continue.");
				printHelp();
				System.exit(-1);
			}
		}
		
//		Need to figure out if param #3 - deploy target - is provided with a directory (and if so, whether we need to create it)
//		or whether it's a file, in which case we need to move/rename the output from the fetch to that target
		
		
		if (cmdlineParams.length >= 3) {
			outputDir = cmdlineParams[2];
			
			// check if this is a valid directory
			
			File checkFile = new File(outputDir);
			if (checkFile.exists()) {
				if (checkFile.isDirectory()) {
					// target provided exists, no need to do anything
				} else {
					// target is a file
					// we will need to overwrite the existing file with whatever we retrieve
					
					outputDir = checkFile.getParentFile().getAbsolutePath();
					outputFilename = checkFile.getName();
					
				}
			} else {
				// target does not exist, check if it looks like a file name (.zip suffix) and act accordingly
				if (outputDir.endsWith(".zip")) {
					
					Pattern filenamePattern = Pattern.compile("(.*?)(\\w*\\.zip).*");
					Matcher m = filenamePattern.matcher(outputDir);
					outputDir = m.group(1);
					outputFilename = m.group(2);
					Utils.checkDir(outputDir);
				} else {
					Utils.checkDir(outputDir);
				}
				
			}
		}
		
		File buildFile = new File("resources/build.xml");
		Project p = new Project();
		p.setProperty("username", connParams.get("username"));
		p.setProperty("password", connParams.get("password"));
		p.setProperty("serverurl", connParams.get("serverurl"));
		p.setProperty("maxPoll", connParams.get("maxPoll"));
		p.setProperty("pollWaitMillis", connParams.get("pollWaitMillis"));
		p.setProperty("dirname", outputDir);
		p.setProperty("packagefile", packageFile);
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
	
	/*
	 * Do an ant deploy
	 */

	private static void executeDeploy(boolean validateOnly) {
		// TODO Auto-generated method stub
		
	}
	
	/*
	 * Read in any org parameters provided in org file, command line, crash out if minimum isn't provided
	 */

	private static void setupOrgParameters() {
		// check if we have a props file, initialize if we do
		
		// read the default file provided first
		
		readOrgParamsFromFile(line.getArgs()[0]);
		
		// now read in if the org params file has been overridden
		
		if (line.hasOption("o") && line.getOptionValue("o") != null && line.getOptionValue("o").length() > 0) {
			readOrgParamsFromFile(line.getOptionValue("o"));
		}
		
		// now read & override with any params we got on the commandline
		
		if (line.hasOption("u") && line.getOptionValue("u") != null && line.getOptionValue("u").length() > 0) {
			connParams.put("username", line.getOptionValue("u"));
		}
		if (line.hasOption("p") && line.getOptionValue("p") != null && line.getOptionValue("p").length() > 0) {
			connParams.put("password", line.getOptionValue("p"));
		}
		if (line.hasOption("s") && line.getOptionValue("s") != null && line.getOptionValue("s").length() > 0) {
			connParams.put("serverurl", line.getOptionValue("s"));
		}
		
		// now check if we have all connection params populated, exist if we don't
		
		for (String propName : managedConnParams) {
			if (connParams.get(propName) == null) {
				System.out.println("Necessary connection parameter " + propName + " not found in org file or on command line. Cannot continue.");
				printHelp();
				System.exit(-1);
			}
		}	
		
	}
	
	/*
	 * Setup the connection parameters we're going to default
	 */

	private static void setupConnParamDefaults() {
		connParams.put("apiversion", "35.0");
		connParams.put("pollWaitMillis", "10000");
		connParams.put("maxPoll", "100");		
	}

	/*
	 * read anything provided in the org props file
	 */
	
	private static void readOrgParamsFromFile(String propsFilename) {
		Properties props = Utils.initProps(propsFilename);
		
		for (String propName : managedConnParams) {
			if (props.getProperty(propName) != null) {
				connParams.put(propName, props.getProperty(propName));
			}
		}	
	}

	private static void setupOptions() {
		
		
		
		commands.addOption( Option.builder("v").longOpt("validate").desc("Validate command (=deploy CheckOnly)").build());
		commands.addOption( Option.builder("d").longOpt("deploy").desc("Deploy command").build());
		commands.addOption( Option.builder("r").longOpt("retrieve").desc("Retrieve command").build());
		options.addOptionGroup(commands);
		options.addOption( Option.builder("o").longOpt( "orgfile" )
                .desc( "file containing org parameters (see below)" )
                .hasArg()
                .build() );	
		options.addOption( Option.builder("u").longOpt( "username" )
		                                .desc( "username for the org (someuser@someorg.com)" )
		                                .hasArg()
		                                .build() );
		options.addOption( Option.builder("p").longOpt( "password" )
                .desc( "password for the org (t0pSecr3t)" )
                .hasArg()
                .build() );
		options.addOption( Option.builder("s").longOpt( "serverurl" )
                .desc( "server URL for the org (https://login.salesforce.com)" )
                .hasArg()
                .build() );
		options.addOption( Option.builder("i").longOpt( "poll" )
                .desc( "polling interval in milliseconds (not mandatory, will default to 10000 if not provided)" )
                .hasArg()
                .build() );
	}

	private static void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
	    formatter.setOptionComparator(null);

	    formatter.printHelp( "migtool [-r|-v|-d] [org parameters file] [zipfile|directory…] [retrieve output destination] [command options...]", options );
	}
	
	private static void printFullManpage() {
		
//		TODO: add small manpage when called without parameters
		
		System.out.println("Migtool is a tool which aims to simplify the steps needed to be able to get at the core functionality of the Force.com migration tool. Essentially, it is to become one of these nice command-line thingies that’s just part of the toolbox, enabling you to deploy & retrieve stuff to/from Salesforce from the Windows command line or Mac/Linux terminal without the hassle of ANT and its somewhat inconvenient way of working with the various functions/parameters of ANT/Metadata API.");
		System.out.println("");
		System.out.println("Basic usage:");
		System.out.println("");
		System.out.println("migtool command [org] [zipfile|directory…] [command options...] ");
		System.out.println("");
		System.out.println("Commands:");
		System.out.println("");
		System.out.println("d - deploy");
		System.out.println("r - retrieve");
		System.out.println("v - validate (deploy with checkonly flag set - equivalent to d -c)");
		System.out.println("");
		System.out.println("Org: specifies a filename/path where migtool will look for a file with connection parameters (see connection parameters below). If it’s a full path, migtool will look for the file there. If it’s a bare filename, it will first look in the current directory, then in the directory specified by the MIGTOOL_PROPS environment variable (if set). If a file is still not found, migtool will append .properties to the filename and look again in the current directory, then in the MIGTOOL_PROPS directory for a match. ");
		System.out.println("");
		System.out.println("If the second parameter (org) does not result in the resolution of a valid file containing at least one org connection parameter according to the logic above, the filename provided will be treated as a file/directory to deploy/retrieve.");
		System.out.println("");
		System.out.println("Any org parameters using the commandline switches (-u, -p, -s) will override anything set in the file. ");
		System.out.println("If a full set of user credentials is not specified using a properties file and/or these three switches, migtool will give up.");
		System.out.println("");
		System.out.println("Connection options (these are for the org and can be instead provided from a file using the org parameter on the command line): ");
		System.out.println("");
		System.out.println("-u username for the org (someuser@someorg.com) ");
		System.out.println("-p password for the org (t0pSecr3t)");
		System.out.println("-s server URL for the org (https://login.salesforce.com)");
		System.out.println("-i polling interval in milliseconds (not mandatory, will default to 10000 if not provided)");
		System.out.println("");
		System.out.println("Org properties file format: ");
		System.out.println("username=someuser@someorg.com");
		System.out.println("password=t0pSecr3t");
		System.out.println("serverurl=https://login.salesforce.com");
		System.out.println("apiversion=35.0");
		System.out.println("pollWaitMillis=10000");
		System.out.println("maxPoll=100");
		System.out.println("");
		System.out.println("");
		System.out.println("Valid deploy command options:");
		System.out.println("");
		System.out.println("-c CheckOnly");
		System.out.println("-t [0|s|l|a]");
		System.out.println("0 - No tests are run ");
		System.out.println("s - specified tests are run (see -tn and -ta options)");
		System.out.println("l - local tests (all tests except for managed package tests are run)");
		System.out.println("a - all tests, including managed package tests are run");
		System.out.println("");
		System.out.println("-tn test1[,test2]… specifies the name of the tests to run (implies -t s flag)");
		System.out.println("-ta “regexp” will tell migtool to look for classes in your package matching the pattern and execute them (can be combined with the -tn flag which will result in the union of the test class names derived being run)");


	}

}
