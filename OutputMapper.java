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
 *        Mapper: OutputMapper (this file)
 *        Reducer: (IdentityReducer by default)
 *     5) Output top 10 PageRank
 *        Mapper: OutputTop10Mapper
 *        Reducer: OutputTop10Reducer
 *        ReducerNum: 1
 *-------------------------------------------------------------------------------
 * OutputMapper (this file):
 *     Input:   Key: (Long) <Line Offset>
 *              Value: (Text) <NodeA> <CurrPR>|<LastPR>$<NodeB1>|<NodeB2>|...
 *     Output:  Key: (Long) <NodeA>
 *              Value: (Double) <CurrPR>
 *================================================================================
 */
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class OutputMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable>{
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        long nodeId = Long.parseLong(line.split("\\s")[0]);
        double pr = Double.parseDouble(line.split("\\s")[1].split("\\|")[0]);
        double each_leak = Double.parseDouble(context.getConfiguration().get("each_leak"));
        pr += each_leak;
        context.write(new LongWritable(nodeId), new DoubleWritable(pr));        
    }
}