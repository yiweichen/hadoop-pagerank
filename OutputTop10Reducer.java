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
 *        Reducer: OutputTop10Reducer (this file)
 *        ReducerNum: 1
 *-------------------------------------------------------------------------------
 * OutputTop10Reducer (this file):
 *     Input:   Key: (Long) 0
 *              Value: (Text) <NodeA> <CurrPR>
 *     Output:  (Top 10)
 *              Key: (Long) <Rank>
 *              Value: (Text) <NodeA> <CurrPR>
 *================================================================================
 */
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class OutputTop10Reducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    
    protected long nodes[] = new long[10];
    protected double prs[] = new double[10];
    
    protected void list_insert(long node, double pr) {
        if (prs[9] >= pr) return;
        int i;
        for (i = 9; i > 0; i--) {
            if (prs[i - 1] >= pr) break;
            prs[i] = prs[i - 1];
            nodes[i] = nodes[i - 1];
        }
        prs[i] = pr;
        nodes[i] = node;
    }
    
    public void reduce(LongWritable key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        for (Text value: values) {
            long nodeId = Long.parseLong(value.toString().split("\\s")[0]);
            double pr = Double.parseDouble(value.toString().split("\\s")[1]);
            list_insert(nodeId, pr);
        }
        for (int i = 0; i < nodes.length; i++) {
            context.write(new LongWritable(i + 1), new Text(String.valueOf(nodes[i]) + "\t" + String.valueOf(prs[i])));            
        }
    }

}