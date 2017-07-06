package edu.rosehulman.configuration;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

//The hadoop helper class to handle and interpret command line options
// is called GenericOptionsParser. It provides the ability to bundle
//options to a configuration object.

//The standard way to do this is to implement the tool interface which
//uses the GenericOptionsParser internally. The tool interface requires
//you to implement Configurable which we do by extending the Configured Subclass.


public class ConfigurationPrinter extends Configured implements Tool{


	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// The configuration can be accessed by using the getConf() method associated with
		//the configurable interface which we implemented by extending the Configured class.
		Configuration conf = getConf();
		System.out.println(conf.get("color"));
		System.out.println(conf.get("size"));
		System.out.println(conf.get("weight"));
		System.out.println(conf.get("size-weight"));
		return 0;
	}
	
	public static void main(String[] args) throws Exception{		
		
		//Rather than calling the tool's run method directly, we call the Toolrunner.run static method
		//which creates a Configuration object and then automatically calls the run method associated with 
		//Tool. It also uses the GenericConfigurationParser to pick up any special options specified using 
		//the command line.
		int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
		System.exit(exitCode);
	}
}