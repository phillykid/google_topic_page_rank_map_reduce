 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Partitioner;

 public class RankIDPartitioner
    extends Partitioner<RankIDPair, Text> {

     @Override
     public int getPartition(RankIDPair pair,
                             Text text,
                             int numberOfPartitions) {
         // make sure that partitions are non-negative
         return Math.abs(pair.getId().hashCode() % numberOfPartitions);
      }
 }