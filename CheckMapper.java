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
 *        Mapper: CheckMapper (this file)
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
 * CheckMapper (this file):
 *     Input:   Key: (Long) <Line Offset>
 *              Value: (Text) <NodeA> <CurrPR>|<LastPR>$<NodeB1>|<NodeB2>|...
 *     Output:  Key: (Text) ChangeRate
 *              Value: (Text) <ChangeRate>
 *              Key: (Text) NodeCount
 *              Value: (Text) 1
 *              Key: (Text) TotalPR
 *              Value: (Text) <CurrPR>
 *================================================================================
 */
import java.io.IOException;
import java.lang.Math;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class CheckMapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("#")) return;
        String[] args = line.split("\\s")[1].split("\\$")[0].split("\\|");
        double pre = Double.parseDouble(args[1]);
        double cur = Double.parseDouble(args[0]);
        double diff = cur - pre;
        double changeRate = Math.abs(diff / pre);
        context.write(new Text("ChangeRate"), new Text(String.valueOf(changeRate)));
        context.write(new Text("NodeCount"), new Text("1"));
        context.write(new Text("TotalPR"), new Text(String.valueOf(cur)));
    }
}
