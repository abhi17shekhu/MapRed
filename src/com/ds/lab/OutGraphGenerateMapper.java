package com.ds.lab;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class OutGraphGenerateMapper extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text> {

    private Text title = new Text();
    private Text link = new Text();
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
	    throws IOException {
	String line = value.toString();
	String[] parts = line.split("\t", -1);
	title.set(parts[0]);
	link.set("");
	if (parts.length > 1) {
	    link.set(parts[1]);
	}
	output.collect(title, link);
    }
}
