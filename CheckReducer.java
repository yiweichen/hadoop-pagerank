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
 *        Reducer: CheckReducer (this file)
 *        ReducerNum: 1
 *     4) Output all PageRank
 *        Mapper: OutputMapper
 *        Reducer: (IdentityReducer by default)
 *     5) Output top 10 PageRank
 *        Mapper: OutputTop10Mapper
 *        Reducer: OutputTop10Reducer
 *        ReducerNum: 1
 *-------------------------------------------------------------------------------
 * CheckReducer (this file):
 *     Input:   Key: (Text) ChangeRate
 *              Value: (Text) <ChangeRate>
 *              Key: (Text) NodeCount
 *              Value: (Text) 1
 *              Key: (Text) TotalPR
 *              Value: (Text) <CurrPR>
 *     Output:  Key: (Text) MaxChangeRate
 *              Value: (Text) <MaxChangeRate>
 *              Key: (Text) NodeCount
 *              Value: (Text) <NodeCount>
 *              Key: (Text) TotalPR
 *              Value: (Text) <TotalPR>
 *================================================================================
 */
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class CheckReducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String keytype = key.toString().trim();
        if (keytype.contains("ChangeRate")) {
            double max = 0.0;
            for (Text val : values) {
                double cur = Double.parseDouble(val.toString());
                if (cur > max) max = cur;
            }
            context.write(new Text("MaxChangeRate"), new Text(String.valueOf(max)));
        } else if (keytype.contains("NodeCount")) {
            int count = 0;
            for (Text val : values) {
                count += Integer.parseInt(val.toString());
            }
            context.write(new Text("NodeCount"), new Text(String.valueOf(count)));
        } else if (keytype.contains("TotalPR")) {
            double pr = 0;
            for (Text val : values) {
                pr += Double.parseDouble(val.toString());
            }
            context.write(new Text("TotalPR"), new Text(String.valueOf(pr)));
        }
    }
}