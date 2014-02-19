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
 *        Reducer: PageRankReducer (this file)
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
 * PageRankReducer (this file):
 *     Input:   Key: (Text) <NodeA>
 *              Value: (Text) 0$<CurrPR>$<NodeB1>|<NodeB2>|...
 *              Key: (Text) <NodeA>
 *              Value: (Text) 1$<PR_received>
 *              Key: (Text) <NodeA>
 *              Value: (Text) 1$<PR_received>
 *              ...
 *     Output:  Key: (Text) <NodeA>
 *              Value: (Text) <NewPR>|<LastPR>$<NodeB1>|<NodeB2>|...
 *================================================================================
 */
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double pr = 0.1;
        double each_leak = Double.parseDouble(context.getConfiguration().get("each_leak"));
        pr += each_leak;
        double old_pr = -1000000.0;
        String toIds = "";
        for (Text val : values) {
            String s = val.toString();
            String[] args = s.split("\\$");
            if (s.startsWith("0")) {
                if (args.length >= 3) {
                    toIds = args[2];
                }
                old_pr = Double.parseDouble(args[1]);
            } else {
                double addpr = Double.parseDouble(args[1]);
                pr += addpr;
            }
        }
        context.write(key, new Text(String.valueOf(pr) + "|" + String.valueOf(old_pr) + "$" + toIds));
    }
}