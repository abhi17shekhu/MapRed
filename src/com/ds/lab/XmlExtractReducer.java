package com.ds.lab;


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class XmlExtractReducer extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterator<Text> values,
	    OutputCollector<Text, Text> output, Reporter reporter)
	    throws IOException {
	HashSet<String> pages = new HashSet<>();
	while (values.hasNext()) {
	    pages.add(values.next().toString());
	}
	if (pages.contains("#")) {
	    output.collect(null , key);
	    pages.remove("#");
	    pages.remove(key.toString());
	    for (String p : pages) {
		output.collect(new Text(p), new Text(key + "\t"));
	    }
	}
    }
}
