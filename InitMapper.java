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
 *        Mapper: InitMapper (this file)
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
 * InitMapper (this file):
 *     Input:   Key: (Long) Line Offset
 *              Value: (Text) <NodeA> <NodeB>
 *     Output:  Key: (Text) <NodeA>
 *              Value: (Text) <NodeB>
 *================================================================================
 */
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("#")) return;
        String fromId = line.split("\\s")[0].trim();
        String toId = line.split("\\s")[1].trim();
        context.write(new Text(fromId), new Text(toId));
    }
}
