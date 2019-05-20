 import org.apache.hadoop.io.WritableComparable;
 import org.apache.hadoop.io.WritableComparator;

 public class RankIDGroupingComparator
    extends WritableComparator {

     public RankIDGroupingComparator() {
         super(RankIDPair.class, true);
     }

     @Override
     /**
      * This comparator controls which keys are grouped
      * together into a single call to the reduce() method
      */
     public int compare(WritableComparable wc1, WritableComparable wc2) {
        RankIDPair pair = (RankIDPair) wc1;
        RankIDPair pair2 = (RankIDPair) wc2;
         return pair.getId().compareTo(pair2.getId());
     }
 }