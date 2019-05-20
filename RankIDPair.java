import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.math.*;

 
  public class RankIDPair
     implements Writable, WritableComparable<RankIDPair> {
 
      private Text pagerank = new Text(); // secondary key
      private Text name = new Text();
      private IntWritable id = new IntWritable(); // natural key
      private BigDecimal current;
      private BigDecimal other;

        @Override
        public String toString(){
            return id +", "+name.toString() +", "+pagerank.toString();

        }

        public Text getPagerank() {
            return pagerank;
        }

        public Text getName() {
            return name;
        }


        public IntWritable getId() {
            return id;
        }


        public void setId(IntWritable id) {
            this.id = id;
        }

        public void setPagerank(Text pagerank) {
            this.pagerank = pagerank;
        }

        public void setName(Text name) {
            this.name = name;
        }


        public void readFields(DataInput dataInput) throws IOException {
            id.set(Integer.parseInt(WritableUtils.readString(dataInput)));
            pagerank.set(WritableUtils.readString(dataInput));
        }
        
        public void write(DataOutput dataOutput) throws IOException {
            WritableUtils.writeString(dataOutput, String.valueOf(id.get()));
            WritableUtils.writeString(dataOutput, pagerank.toString());
        }

    @Override
        /**
         * This comparator controls the sort order of the keys.
         */
    public int compareTo(RankIDPair pair) {

            current = new BigDecimal(this.pagerank.toString());
            other = new BigDecimal(pair.getPagerank().toString());
            

            int compareValue = current.compareTo(other);
            if (compareValue == 0) {
                compareValue = pair.getId().compareTo(this.id);
            }
                    //return compareValue;    // sort ascending
            return -1*compareValue;   // sort descending
        }
        
    }