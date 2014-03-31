package com.spnotes.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount2Reducer extends Reducer<Text, Text, Text, Text>{

	Logger logger = LoggerFactory.getLogger(WordCount2Reducer.class);
	
	private Text highestKey;
	private int highestCount;
	
	@Override
	protected void reduce(Text key, Iterable<Text> list,
			Context context)
			throws IOException, InterruptedException {
		logger.debug("Entering WordCount2Reducer.reduce()");
		
		Text value = list.iterator().next();
		
		if(highestCount == 0){
			highestCount = Integer.parseInt(value.toString());
			highestKey = key;
		}else if(Integer.parseInt(value.toString()) > highestCount){
			highestCount = Integer.parseInt(value.toString());
			highestKey = key;
		}
		
		logger.debug("Exiting WordCount2Reducer.reduce()");
	}

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		context.write(highestKey, new Text(highestCount+""));
	}

	
}
