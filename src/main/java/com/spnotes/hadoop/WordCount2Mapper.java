package com.spnotes.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount2Mapper extends Mapper<Text, Text, Text, Text>{

	Logger logger = LoggerFactory.getLogger(WordCount2Mapper.class);
	
	@Override
	protected void map(Text key, Text value,
			Context context)
			throws IOException, InterruptedException {
		logger.debug("Entering WordCount2Mapper.map()");
		context.write(key, value);
		logger.debug("Exiting WordCount2Mapper.map()");
	}

}
