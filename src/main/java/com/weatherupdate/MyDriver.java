package com.weatherupdate;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;



public class MyDriver extends Configured implements Tool {

	private static final Logger logger=LoggerFactory.getLogger(MyDriver.class);
	private static final long DEFAULT_SPLIT_SIZE=FileUtils.ONE_GB * 4;
	private String jobName = "MyDriver";

	public static void main(String[] args) throws Exception {
		/*if (args.length != 2) {
              System.out.println("usage: [input] [output]");
              else*/
		ToolRunner.run(new Configuration(),new MyDriver(), args);
		System.exit(-1);
	}

	private void parseArgs(String[] args,Job job ,FileSystem fs) throws Exception {
		//setting the InputPath 


		MultipleInputs.addInputPath(job, new Path(this.parseFoundationTableArg(args)),TextInputFormat.class, MyMapper.class);
		MultipleInputs.addInputPath(job, new Path(this.parseIncrementalTableArg(args)),TextInputFormat.class, MyMapper.class);
		Path outputDir = new Path(this.parseOutputDirectoryArg(args));
		this.deleteOutputDirectory(fs,outputDir);
		TextOutputFormat.setOutputPath(job, outputDir);

		//    this.loadClassPath(this.parseLibraryDirectoryArg(args),job);

	}

	/*

    public void loadClassPath(String libPath,Job job)
    {
    logger.info("Enter method");
    Path file=null;
    LocatedFileStatus fileStatus=null;
    RemoteIterator<LocatedFileStatus> iter=null;

    //Mount HDFS
    FileSystem hdfs=FileSystem.get(job.getConfiguration());
    logger.info("HDFS FileSystem mounted");

    //Copy(override ) jar file to HDFS
    iter=hdfs.


    }


 private String parseLibraryDirectoryArg(String[] args)
    {
     String libraryDirectory=null;

     if (args.length >= 3 && null!=args[2])
     {

        libraryDirectory=args[2];
        logger.info("Library Directory" + libraryDirectory);

     }
     else
     {
        logger.info("ERROR : No library Directory specified");
     }

    return libraryDirectory;
    }


	 */



	private void deleteOutputDirectory(FileSystem fs,Path directory){
		try {

			if (fs.exists(directory)) {
				logger.info("Found" + directory.toString());
				fs.delete(directory,true);
				logger.info( "Deleted" + directory.toString());
			}else
			{
				logger.info(directory.toString() + "Doesnt exist");
			}
		} catch ( Exception ex)
		{
			logger.error (directory.toString() + "Doesnt exists");
		}

	}
	
	
	private String parseFoundationTableArg(String[] args){
		int index=0;
		String inputDirectory1=null;
		
		if ( args.length > index)
		{
			inputDirectory1=args[0];
			logger.info("Input Directory1 " + inputDirectory1);
            
	    }
		else
			logger.info("ERROR : No Input Directory Specified " );
		
		return inputDirectory1;
      
	}

	private String parseIncrementalTableArg(String[] args){
		int index=0;

		String inputDirectory2=null;
		if ( args.length > index)
		{
			inputDirectory2=args[1];
			logger.info("Input Directory1 " + inputDirectory2);
		}
		else
			logger.info("ERROR : No Input Directory Specified " );
		
		return inputDirectory2;

	}



	private String parseOutputDirectoryArg(String[] args){
		int index=1;
		String outputDirectory=null;
		if ( args.length > index)
		{
			outputDirectory=args[2];
			logger.info("Output Directory " + outputDirectory);
		}
		else
			logger.info("ERROR : No Output Directory Specified " );
		
		return outputDirectory; 

	}

	public int run ( String[] args) throws Exception {

		Configuration conf=this.getConf();
		Job job=this.getjob(conf);

		if (null != job)
		{

			FileSystem fs =FileSystem.get(conf);
			this.parseArgs(args,job,fs);
			job.waitForCompletion(true);
		}

		return 0;
	}

	private Job getjob(Configuration config) {

		Job job=null;
		try {

			job = Job.getInstance(new Configuration());
			job.setMapSpeculativeExecution(false);
			TextInputFormat.setMinInputSplitSize(job,4294967296L);
			TextInputFormat.setMaxInputSplitSize(job,4294967296L);

			if (System.getenv("HADOOP_TOKEN_FILE_LOCATION ") !=null )
			{

				job.getConfiguration().set("mapreduce.job.credentials.binary",System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
			}

			job.setJarByClass(MyDriver.class);
			//setting the classes
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);

			//reducer format
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(CustomKey.class);
			job.setMapOutputValueClass(TextInputFormat.class);

			//setting the input format 

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);


			job.submit();
			
			

		}catch ( IOException | ClassNotFoundException | InterruptedException e)
		{
			logger.error("An error has occured while trying to create the hadoop object");
		}

		return job;

      }
}


