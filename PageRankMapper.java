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
 *        Mapper: PageRankMapper (this file)
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
 * PageRankMapper (this file):
 *     Input:   Key: (Long) <Line Offset>
 *              Value: (Text) <NodeA> <CurrPR>|<LastPR>$<NodeB1>|<NodeB2>|...
 *     Output:  Key: (Text) <NodeA>
 *              Value: (Text) 0$<CurrPR>$<NodeB1>|<NodeB2>|...
 *              Key: (Text) <NodeB1>
 *              Value: (Text) 1$<PR_received>
 *              Key: (Text) <NodeB2>
 *              Value: (Text) 1$<PR_received>
 *              ...
 *================================================================================
 */
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String fromId = line.split("\\s")[0];
        String argstr = line.split("\\s")[1];
        String[] args = argstr.split("\\$");
        double total = Double.parseDouble(args[0].split("\\|")[0]);
        String toIdstr = "";
        String[] toIds = {};
        if (args.length >= 2) {
            toIdstr = args[1];
            toIds = toIdstr.split("\\|");
        }
        context.write(new Text(fromId), new Text("0$" + args[0].split("\\|")[0] + "$" + toIdstr));
        double each = (total * 0.9) / toIds.length;
        
        for (String toId : toIds) {
            context.write(new Text(toId), new Text("1$" + String.valueOf(each)));
        }
    }
}