package com.ds.lab;



import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class PageRank {
    private static String numberOfPages;
    private static String initialized = "false";
    final private static double epsilon = 1.0e-8;
    private static long oldCount = 1;
    public static enum Counter {
        CONVERGENCE_COUNTER
    }

    private static HashMap<String, String> outPaths;

    public static void initialFiles (String outBucketName) {
        outPaths = new HashMap<String, String>();
        String tmpPathName = outBucketName + "/temp/";
        String resultPathName = outBucketName + "/graph/";
        outPaths.put("outlinkRes", resultPathName + "PageRank.outlink.out");
        outPaths.put("nRes", resultPathName + "PageRank.n.out");
        outPaths.put("iterRes", resultPathName + "PageRank.out");
        outPaths.put("job1Tmp", tmpPathName + "job1/");
        outPaths.put("job2Tmp", tmpPathName + "job2/");
    }

    // Job 1: extract outlink from XML file and remove red link
    public static void parseXml(String input, String output) throws Exception {
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("xmlextract");
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        conf.setInputFormat(XmlInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setMapperClass(XmlExtractMapper.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setReducerClass(XmlExtractReducer.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
    }

    // Job 2: Generate adjacency outlink graph
    public static void getAdjacencyGraph(String input, String output) throws Exception {
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("outgraphgenerate");

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(OutGraphGenerateMapper.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setReducerClass(OutGraphGenerateReducer.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
    }

    @SuppressWarnings("unchecked")
    public static class PageRankGenericWritable extends GenericWritable {

        private static Class<? extends Writable>[] CLASSES = null;

        static {
        CLASSES = new Class[] { Text.class, DoubleWritable.class };
        }

        // this empty initialize is required by Hadoop
        public PageRankGenericWritable() {
        }

        public PageRankGenericWritable(Writable instance) {
        set(instance);
        }

        @Override
        protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
        }

        @Override
        public String toString() {
        return this.get().toString();
        }
    }


    public static class DescDoubleComparator extends WritableComparator {

        protected DescDoubleComparator() {
        super(DoubleWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable d1 = (DoubleWritable) w1;
        DoubleWritable d2 = (DoubleWritable) w2;
        return -d1.compareTo(d2);
        }

    }


    public static void main(String[] args) throws Exception {
        // initialization
        // args[0]: input s3://uf-dsr-courses-ids/data/enwiki-latest-pages-articles.xml
        // args[1]: output s3://rui-zhang-emr/
        Configuration conf = new Configuration();
        FileSystem fs =  FileSystem.get(new URI(args[1]), conf);
        PageRank.initialFiles(args[1]);
        //extract wiki and remove redlinks
        PageRank.parseXml(args[0], outPaths.get("job1Tmp"));
        // wiki adjacency graph generation
        PageRank.getAdjacencyGraph(outPaths.get("job1Tmp"), outPaths.get("job2Tmp"));
        FileUtil.copyMerge(fs, new Path(outPaths.get("job2Tmp")), fs, new Path(outPaths.get("outlinkRes")), false, conf, "");
        
    }
}



