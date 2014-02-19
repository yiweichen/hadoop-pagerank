/*================================================================================
 * Name: PageRank Calculator
 * Author: Yiwei CHEN
 * Job Type: Hadoop Custom JAR
 * Description:
 *     Calculate PageRank values of all nodes, then find the top 10.
 *-------------------------------------------------------------------------------
 * Usage:
 *     hadoop jar <JAR Path> <Input Path> <Output Path>
 *-------------------------------------------------------------------------------
 * MapReduce Jobs:
 *     1) Initialization
 *        Mapper: InitMapper
 *        Partitioner: KeyFieldBasedPartitioner
 *        Reducer: InitReducer
 *     2) PageRank calculation (iterated)
 *        Mapper: PageRankMapper
 *        Partitioner: KeyFieldBasedPartitioner
 *        Reducer: PageRankReducer
 *     3) Results check (iterated)
 *        Mapper: CheckMapper
 *        Reducer: CheckReducer
 *        ReducerNum: 1
 *     4) Output all PageRank
 *        Mapper: OutputMapper
 *        Reducer: (IdentityReducer by default)
 *     5) Output top 10 PageRank
 *        Mapper: OutputTop10Mapper
 *        Reducer: OutputTop10Reducer
 *        ReducerNum: 1
 *-------------------------------------------------------------------------------
 * PageRank (this file):
 *     The jar entry class and hadoop job driver. The PageRank values calculation
 * iterates until the maximum of all nodes' change rates of PageRank is less than
 * 1%. All the intermediate data between each time of iteration are store under
 * /tmp/_prtmp/ in the HDFS and will be cleaned up after the calculation is done.
 * The program will produce /all and /top10 directories containing all nodes'
 * PageRank and the top 10 node respectively.
 *================================================================================
 */
import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedPartitioner;
import org.apache.hadoop.util.LineReader;

public class PageRank {
    private static String inputPath;
    private static String outputPath;
    private static String tmpPath = "/tmp/_prtmp/";
    private static NumberFormat format = NumberFormat.getInstance();
    private static int iterationNum = 0;
    
    private static double maxChangeRate = 0;
    private static int nodeCount = 0;
    private static double totalPR = 0;
    
    /* run initialization MapReduce job */
    private static void init() throws Exception {
        iterationNum = 0;
        Configuration conf = new Configuration();
        
        Job job = new Job(conf, "PageRank_init");
        job.setJarByClass(PageRank.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(InitMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(KeyFieldBasedPartitioner.class);
        job.setReducerClass(InitReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job,
                new Path(tmpPath + format.format(iterationNum) + "/"));
        
        job.waitForCompletion(true);
    }
    
    /* run one PageRank iteration */
    private static void calc() throws Exception {
        Configuration conf = new Configuration();
        if (nodeCount > 0) {
            conf.set("each_leak", String.valueOf((nodeCount - totalPR) / nodeCount));
        } else {
            conf.set("each_leak", "0");
        }
        
        Job job = new Job(conf, "PageRank_" + format.format(iterationNum));
        job.setJarByClass(PageRank.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(PageRankMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(KeyFieldBasedPartitioner.class);
        job.setReducerClass(PageRankReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job,
                new Path(tmpPath + format.format(iterationNum) + "/"));
        FileOutputFormat.setOutputPath(job,
                new Path(tmpPath + format.format(iterationNum + 1) + "/"));
        
        job.waitForCompletion(true);
        iterationNum++;
    }
    
    /* run result-check MapReduce job, calculate max change rate, etc */
    private static void check() throws Exception {
        Configuration conf = new Configuration();
        
        Job job = new Job(conf, "PageRank_" + format.format(iterationNum) + "_check");
        job.setJarByClass(PageRank.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(CheckMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(KeyFieldBasedPartitioner.class);
        job.setCombinerClass(CheckReducer.class);
        job.setReducerClass(CheckReducer.class);
        job.setNumReduceTasks(1);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job,
                new Path(tmpPath + format.format(iterationNum) + "/"));
        FileOutputFormat.setOutputPath(job,
                new Path(tmpPath + format.format(iterationNum) + "_check/"));
        job.waitForCompletion(true);
                
    }
    
    /* read max check results from file */
    private static void readChangeRate() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        Path p = new Path(tmpPath + format.format(iterationNum) + "_check/part-r-00000");
        FSDataInputStream is = fs.open(p);
        LineReader in = new LineReader(is);
        Text text = new Text();
        while (in.readLine(text) > 0) {
            String[] strs = text.toString().split("\\s");
            if (strs[0].contains("MaxChangeRate")) {
                maxChangeRate = Double.parseDouble(strs[1]);
            } else if (strs[0].contains("NodeCount")) {
                nodeCount = Integer.parseInt(strs[1]);
            } else if (strs[0].contains("TotalPR")) {
                totalPR = Double.parseDouble(strs[1]);
            }
        }
        in.close();
        is.close();
    }
    
    /* run output all PageRank MapReduce job */
    private static void outputAll() throws Exception {
        Configuration conf = new Configuration();
        conf.set("each_leak", String.valueOf((nodeCount - totalPR) / nodeCount));
        
        Job job = new Job(conf, "PageRank_all");
        job.setJarByClass(PageRank.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(OutputMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job,
                new Path(tmpPath + format.format(iterationNum) + "/"));
        FileOutputFormat.setOutputPath(job,
                new Path(outputPath + "/all/"));
        job.waitForCompletion(true);
    }
    
    /* run output top 10 PageRank MapReduce job */
    private static void outputTop10() throws Exception {
        Configuration conf = new Configuration();
        
        Job job = new Job(conf, "PageRank_top10");
        job.setJarByClass(PageRank.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(OutputTop10Mapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(OutputTop10Reducer.class);
        job.setNumReduceTasks(1);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job,
                new Path(outputPath + "/all/"));
        FileOutputFormat.setOutputPath(job,
                new Path(outputPath + "/top10/"));
        job.waitForCompletion(true);
    }
    
    /* clean up intermediate data */
    private static void cleanUp() throws IOException{
        FileSystem fs = FileSystem.get(new Configuration());
        Path p = new Path(tmpPath);
        fs.delete(p, true);
    }
    
    /* program entry */
    public static void main(String[] args) throws Exception {
        inputPath = args[0];
        outputPath = args[1];
        format = NumberFormat.getInstance();
        format.setGroupingUsed(false);
        format.setMaximumIntegerDigits(4);
        format.setMinimumIntegerDigits(4);
        
        init();
        for (int i = 0; i < 200; i++) {
            calc();
            check();
            readChangeRate();
            if (maxChangeRate < 0.01) break;
        }
        outputAll();
        outputTop10();
        cleanUp();
    }
}
