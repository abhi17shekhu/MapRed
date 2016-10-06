package com.ds.lab;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class XmlExtractMapper extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text> {

    static Pattern titlePattern = Pattern.compile("<title>(.*?)</title>", Pattern.DOTALL);
    static Pattern textPattern = Pattern.compile("<text[^>]*>(.*?)</text>", Pattern.DOTALL);
    static Pattern linkPattern = Pattern.compile("\\[{2}([^|\\[\\]]*)", Pattern.DOTALL);
    private Text title = new Text();
    private Text link = new Text();
    private Text mark = new Text("#");

    @Override
    public void map(LongWritable key, Text value,
	    OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
	String line = value.toString();
	Matcher titleMathcer = titlePattern.matcher(line);
	if (titleMathcer.find()) {
	    String titleStr = titleMathcer.group(1).replace(' ', '_');
	    title.set(titleStr.replaceAll("&amp;", "&").replaceAll("&quot;", "\""));
	    output.collect(title, mark);
	}
	Matcher textMathcer = textPattern.matcher(line);
	if (textMathcer.find()) {
	    String text = textMathcer.group(1);
	    Matcher linkMathcer = linkPattern.matcher(text);
	    while (linkMathcer.find()) {
		String linkStr = linkMathcer.group(1).replace(' ', '_');
		link.set(linkStr.replaceAll("&amp;", "&").replaceAll("&quot;", "\""));
		output.collect(link, title);
	    }
	}
    }
}
