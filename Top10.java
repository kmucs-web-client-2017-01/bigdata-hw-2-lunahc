package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONObject;

public class Top10 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        
        int res = ToolRunner.run(new Configuration(), new Top10(), args);
        
        System.exit(res);
    }
    
    public static class DescendingIntWritableComparable extends IntWritable {
        /** A decreasing Comparator optimized for IntWritable. */ 
        public static class DecreasingComparator extends Comparator {
            public int compare(WritableComparable a, WritableComparable b) {
                return -super.compare(a, b);
            }
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                return -super.compare(b1, s1, l1, b2, s2, l2);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        
        // jobCountWordPair
        Job jobCountWordPair = Job.getInstance(getConf());
        jobCountWordPair.setJarByClass(Top10.class);
        jobCountWordPair.setOutputKeyClass(Text.class);
        jobCountWordPair.setOutputValueClass(IntWritable.class);
        
        jobCountWordPair.setMapperClass(MapCountWordPair.class);
        jobCountWordPair.setReducerClass(ReduceCountWordPair.class);
        
        jobCountWordPair.setInputFormatClass(TextInputFormat.class);
        jobCountWordPair.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(jobCountWordPair, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobCountWordPair, new Path(args[1]+"/countWordPair"));
        
        jobCountWordPair.setNumReduceTasks(10);
        
        jobCountWordPair.waitForCompletion(true);
        
        // jobGetTop10
        Job jobGetTop10 = Job.getInstance(getConf());
        jobGetTop10.setJarByClass(Top10.class);
        jobGetTop10.setOutputKeyClass(IntWritable.class);
        jobGetTop10.setOutputValueClass(Text.class);
        
        jobGetTop10.setMapperClass(MapGetTop10.class);
        jobGetTop10.setReducerClass(ReduceGetTop10.class);
        
        jobGetTop10.setInputFormatClass(TextInputFormat.class);
        jobGetTop10.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(jobGetTop10, new Path(args[1]+"/countWordPair"));
        FileOutputFormat.setOutputPath(jobGetTop10, new Path(args[1]+"/result"));
        
        jobGetTop10.setSortComparatorClass(DescendingIntWritableComparable.DecreasingComparator.class);
        jobGetTop10.setNumReduceTasks(10);
        
        jobGetTop10.waitForCompletion(true);
        
        return 0;
    }
    
    public static class MapCountWordPair extends Mapper<LongWritable, Text, Text, IntWritable> {
    	private String[] targetWords = {"text", "refers", "edition", "unavailable", "print", "one", "business", "book", "read", "series"}; 
    	
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	try {
        		JSONObject jsonObject = new JSONObject(value.toString());
        		String[] words = jsonObject.getString("description").split("\\s+");
                
                ArrayList<String> existWords = new ArrayList<String>();
                for (int i=0; i<words.length; i++)
                	for (String targetWord : targetWords)
                		if (words[i].equals(targetWord))
                			existWords.add(targetWord);
                
                if (existWords.size() == 0)
                	return ;
                
                search:
                for (int i=0; i<words.length; i++){
                	for (String target : existWords)
                		if (target.equals(words[i]))
                			continue search;
                	
                	for (String target : existWords)
                		context.write(new Text(target + "," + words[i]), new IntWritable(1));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReduceCountWordPair extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        	int count = 0;
        	
            for (IntWritable val : values)
            	count++;
            
            context.write(key, new IntWritable(count));
        }
    }
    
    public static class MapGetTop10 extends Mapper<LongWritable, Text, IntWritable, Text> { 
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	String[] wordPairCount = value.toString().split("\t");
        	
        	context.write(new IntWritable(Integer.parseInt(wordPairCount[1])), new Text(wordPairCount[0]));
        }
    }

    public static class ReduceGetTop10 extends Reducer<IntWritable, Text, Text, IntWritable> {
    	private Map<String, Integer> count = new HashMap<String, Integer>();
    	
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	
        	for (Text wordPair : values) {
        		String[] words = wordPair.toString().split(",");
        		
        		if (!count.containsKey(words[0]))
        			count.put(words[0], 1);
        		else if(count.get(words[0]) >= 10)
        			continue;
        		else
        			count.put(words[0], count.get(words[0])+1);
        		
        		context.write(new Text(words[0] + " " + words[1]), key);
			}
        }
    }
}