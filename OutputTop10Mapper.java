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
 *        Mapper: OutputTop10Mapper (this file)
 *        Reducer: OutputTop10Reducer
 *        ReducerNum: 1
 *-------------------------------------------------------------------------------
 * OutputTop10Mapper (this file):
 *     Input:   Key: (Long) <Line Offset>
 *              Value: (Text) <NodeA> <CurrPR>|<LastPR>$<NodeB1>|<NodeB2>|...
 *     Output:  Key: (Long) 0
 *              Value: (Text) <NodeA> <CurrPR>
 *================================================================================
 */
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class OutputTop10Mapper extends Mapper<LongWritable, Text, LongWritable, Text>{
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        long nodeId = Long.parseLong(line.split("\\s")[0]);
        double pr = Double.parseDouble(line.split("\\s")[1]);
        context.write(new LongWritable(0), new Text(String.valueOf(nodeId) + "\t" + String.valueOf(pr)));        
    }
}