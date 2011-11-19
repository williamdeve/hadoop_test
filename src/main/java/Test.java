
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Test {
	public  static class map extends Mapper<LongWritable, Text, Text, LongWritable>{

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if(line ==null || "".equals(line)){
				return;
				
			}
			String[] words = line.split(",");
			if(words==null || words.length<8){
				return;
			}
			String appId= words[0];
			String appName=words[1];
			LongWritable recordBytes = new LongWritable(Long.parseLong(words[7]));
			Text  record = new Text();
			record.set(new StringBuffer("flow::").append(appId).append("::").append(appName).toString()); 
			context.write(record, recordBytes);
			record.clear();
			record.set(new StringBuffer("count::").append(appId).append("::").append(appName).toString()); 
			context.write(record, new LongWritable(1));
		}
		
	}
	public static class partionerExt extends Partitioner<Text, LongWritable>{

		public int getPartition(Text key, LongWritable value, int partionNumbers) {
			// TODO Auto-generated method stub
			if(partionNumbers>=2){
				if(key.toString().startsWith("flow::")){
					return 0;
				}else{
					return 1;
				}
			}
			return 0;
		}
		public void configure(Job job){
			
		}
		
	}
	public static class reduce extends Reducer<Text, LongWritable, Text, LongWritable>{

		public void reduce(Text key, Iterable<LongWritable> values,Context context)throws IOException, InterruptedException {
			Text newkey = new Text();  
			  
            newkey.set(key.toString().substring(key.toString().indexOf("::") + 2)); 
			LongWritable result = new LongWritable();
			Iterator<LongWritable> valueIter =values.iterator();
			long tmp=0;
			long count=0;
			while(valueIter.hasNext()){
				tmp+=valueIter.next().get();
				count++;
				if((count=count%1000)==0){
					context.progress();
				}
				
			}
			result.set(tmp);
			context.write(newkey, result);
		}

		
	}
	public static class combiner extends Reducer<Text, LongWritable, Text, LongWritable>{

		public void reduce(Text key, Iterable<LongWritable> values,Context context)throws IOException, InterruptedException {
			Text newkey = new Text();  
			  
            newkey.set(key.toString().substring(key.toString().indexOf("::") + 2)); 
			LongWritable result = new LongWritable();
			Iterator<LongWritable> valueIter =values.iterator();
			long tmp=0;
			while(valueIter.hasNext()){
				tmp+=valueIter.next().get();
			}
			result.set(tmp);
		}

		
	}
	 public static void main(String[] args){
		 if(args==null || args.length<2){
			 System.out.println("no input and output folder");  
			 return;
		 }
		 String inPath= args[0];
		 String outPath =args[1];
		 String inShort=inPath;
		 String outShort=outPath;
		 if (inShort.indexOf(File.separator) >= 0)  
			 inShort = inShort.substring(inShort.lastIndexOf(File.separator));
		 
	     if (outShort.indexOf(File.separator) >= 0)  
	         outShort = outShort.substring(outShort.lastIndexOf(File.separator)); 
	     
	     SimpleDateFormat formater = new SimpleDateFormat("yyyy.MM.dd");  
	     outShort = new StringBuffer(outShort).append("-").append(formater.format(new Date())).toString(); 
	     
		if (!inShort.startsWith("/"))
			inShort = "/" + inShort;

		if (!outShort.startsWith("/"))
			outShort = "/" + outShort;

		inShort = "/user/root" + inShort;
		outShort = "/user/root" + outShort;
		
		File inputdir = new File(inPath);  
		  
        File outputdir = new File(outPath);  
  
        if (!inputdir.exists() || !inputdir.isDirectory()){  
            System.out.println("inputpath not exist or isn't dir!");  
            return;  
        }  
        if (!outputdir.exists()){  
            new File(outPath).mkdirs();  
        } 
        Configuration conf = new Configuration();
        try {
			Job job= new Job(conf,"test job");
			job.setJarByClass(Test.class);
			FileSystem fileSys = FileSystem.get(conf); 
			fileSys.copyFromLocalFile(new Path(inPath), new Path(inShort));
			job.setJobName("test");  
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class); 
			job.setMapperClass(map.class);
			job.setCombinerClass(combiner.class); 
			job.setReducerClass(reduce.class);  
	        job.setPartitionerClass(partionerExt.class); 
	        
	        job.setNumReduceTasks(2);
	        FileInputFormat.setInputPaths(job, inShort);
	        FileOutputFormat.setOutputPath(job, new Path(outShort));
	        Date startTime = new Date();  
	        System.out.println("Job started: " + startTime);  
	        Date end_time = new Date();  
	        
	        System.out.println("Job ended: " + end_time);  
	  
	        System.out.println("The job took "  
	                + (end_time.getTime() - startTime.getTime()) / 1000  
	                + " seconds.");  
	        fileSys.copyToLocalFile(new Path(outShort), new Path(outPath));  
	        fileSys.delete(new Path(inShort), true);  
	        fileSys.delete(new Path(outShort), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 }
}
