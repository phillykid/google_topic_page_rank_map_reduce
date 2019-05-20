import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.util.Scanner;
import java.util.Comparator;
import java.math.*;
import java.util.regex.*;
import org.apache.hadoop.io.NullWritable;
import java.nio.file.Paths;
import java.net.URL;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;


public class TopicPageRank {

    private static ArrayList < Integer > topic_pages_table = new ArrayList < > ();
    private static BigDecimal teleport_divided = new BigDecimal(0.00065217391); //.15/230 1-B/topic_pages


  
        private static BufferedReader pg_scanner; 
        private static BufferedReader topic_scanner;
        private static URL topics; 
        private static URL names; 
        
  
  


    public enum KEEPALIVECOUNTER {
        ALIVE
    };

    public static class InitialTopicPageRankMapper
    extends Mapper < Object, Text, IntWritable, IntWritable > {

        IntWritable source_node = new IntWritable(0);
        IntWritable destination_node = new IntWritable(0);
        String[] splits;


        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {



            splits = value.toString().split("\\s+");


            source_node.set(Integer.parseInt(splits[0]));
            destination_node.set(Integer.parseInt(splits[1]));



            context.write(source_node, destination_node);


        }
    }



    /**
     *  This is the mapper used to do the bulk of the calculations.
     *  Input Example: 1297 [10,6,7,8]::0.00054623442442) node [destination nodes]::pagerank
     *  OutPuts Example: 10 0.00054623442442/out degree of 1297 && 1297 [10,6,7,8]
     *  One output is the pagerank contribution for the destination nodes 
     *  The other output is the source node and its destination strip which is needed for continued calculations. 
     * 
     */
    public static class TopicPageRankMapper
    extends Mapper < Object, Text, IntWritable, Text > {

        Text pagerank = new Text("");
        Text destination_strip = new Text("");
        IntWritable source_node = new IntWritable(0);
        IntWritable destination_node = new IntWritable(0);
        BigDecimal passed_page_rank;
        BigDecimal divided_page_rank;
        String[] splits;
        String[] split;
        String[] nodes;
        String regex_feed;
        String passed_page_rank_string;
        String destination_strip_string;

        private ArrayList < Integer > destinations;
        Pattern pattern;
        Matcher matcher;




        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {


            regex_feed = value.toString();

            pattern = Pattern.compile("\\d+\\s");
            matcher = pattern.matcher(regex_feed);
            matcher.find();
            source_node.set(Integer.parseInt(matcher.group().trim()));

            pattern = Pattern.compile("\\[.*?(\\d+).*\\]");
            matcher = pattern.matcher(regex_feed);
            matcher.find();
            destination_strip_string = matcher.group();
            nodes = destination_strip_string.substring(1, destination_strip_string.length() - 1).split(",");


            pattern = Pattern.compile("\\d+\\.+\\d+\\w");
            matcher = pattern.matcher(regex_feed);
            matcher.find();


            passed_page_rank = new BigDecimal(matcher.group());
            divided_page_rank = passed_page_rank.divide(new BigDecimal(nodes.length), 100, RoundingMode.HALF_UP);
            divided_page_rank = divided_page_rank.multiply(new BigDecimal(0.85));



            if (regex_feed.contains("Topic")) {
                destination_strip.set(destination_strip_string + "Topic*");
                divided_page_rank.add(teleport_divided);
            } else {
                destination_strip.set(destination_strip_string + "*");
            }

            pagerank.set(divided_page_rank.toPlainString());


            for (String node: nodes) {
                destination_node.set(Integer.parseInt(node.trim()));
                context.write(destination_node, pagerank);

            }




            context.write(source_node, destination_strip);



        }

    }


    /**
     *  This is the final mapper which only passes on the node id and its calculated page rank.
     *  Input Example: 1297 [10,6,7,8]::0.00054623442442) node [destination nodes]::pagerank
     *  OutPut Example: 1297 0.00054623442442
     */
    public static class TopicCombinerMapper
    extends Mapper < Object, Text, RankIDPair, NullWritable > {

        Text pagerank = new Text("");
        Text destination_strip = new Text("");
        IntWritable source_node = new IntWritable(0);
        IntWritable destination_node = new IntWritable(0);
        BigDecimal passed_page_rank;
        BigDecimal divided_page_rank;
        String[] splits;
        String[] split;
        String[] nodes;
        String regex_feed;
        String passed_page_rank_string;
        String destination_strip_string;

        private ArrayList < Integer > destinations;
        Pattern pattern;
        Matcher matcher;




        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {


            regex_feed = value.toString();

            pattern = Pattern.compile("\\d+\\s");
            matcher = pattern.matcher(regex_feed);
            matcher.find();
            source_node.set(Integer.parseInt(matcher.group().trim()));


            pattern = Pattern.compile("\\d+\\.+\\d+\\w");
            matcher = pattern.matcher(regex_feed);
            matcher.find();


            pagerank.set(matcher.group());


            RankIDPair reducerKey = new RankIDPair();
            reducerKey.setId(source_node);
            reducerKey.setPagerank(pagerank);







            context.write(reducerKey, NullWritable.get());



        }

    }


    public static class TopicCombinerReducer
    extends Reducer < RankIDPair, NullWritable, RankIDPair, NullWritable > {
        Text name = new Text("");
        BigDecimal holder;
        BigDecimal sum;
        String dest_holder;
        String line = new String();



        Map < String, String > page_name_table = new HashMap < String,String > ();











        public void reduce(RankIDPair key, Iterable < NullWritable > values,
            Context context
        ) throws IOException,
        InterruptedException {


            if (page_name_table.isEmpty()) {
                try{
                URL names = new URL("https://sfo2.digitaloceanspaces.com/pagerank-417/wiki-topcats-page-names.txt");
                pg_scanner = new BufferedReader(new InputStreamReader(names.openStream())); 
                while ((line = pg_scanner.readLine()) != null) {
                    String[] data = line.split("\\s+", 2);
                    String id = data[0];
                    String name = data[1];
                    page_name_table.put(id, name);
                    
                }
                pg_scanner.close();
            }catch(Exception e){
                e.printStackTrace();
            }
            }


            //Combines page names with the rest of the information
            if (page_name_table.containsKey(key.getId().toString())) {
                name.set(page_name_table.get(key.getId().toString()));
                key.setName(name);


            } else {
                System.out.println("Not Found");

            }





            context.write(key, NullWritable.get());









        }
    }

    public static class TopicPageRankReducer
    extends Reducer < IntWritable, Text, IntWritable, Text > {
        private final static Text info_text = new Text("");
        BigDecimal holder;
        BigDecimal sum;
        String dest_holder;



        public void reduce(IntWritable key, Iterable < Text > values,
            Context context
        ) throws IOException,
        InterruptedException {

            sum = new BigDecimal(0.0000000000000000000000000);
            dest_holder = "NULL";

            for (Text value: values) {
                context.getCounter(KEEPALIVECOUNTER.ALIVE).increment(1);

                String checker = value.toString();

                if (checker.contains("*")) {
                    dest_holder = checker.substring(0, checker.length() - 1);


                } else {
                    holder = new BigDecimal(checker);
                    sum = sum.add(holder);
                }

            }




            String info_string = dest_holder + "::" + sum.toPlainString();
            info_text.set(info_string);


            context.write(key, info_text);


        }
    }







    public static class InitialTopicPageRankReducer
    extends Reducer < IntWritable, IntWritable, IntWritable, Text > {
        private final static Text info_text = new Text("");
        private ArrayList < Integer > destinations;




        String line = new String();
        String info_string;
        BigDecimal inital_page_rank;
       

        
        


        public void reduce(IntWritable key, Iterable < IntWritable > values,
            Context context
        ) throws IOException,
        InterruptedException {
            destinations = new ArrayList < > ();
            
            int node;

            if (topic_pages_table.isEmpty()) {
                try{
                URL topics = new URL("https://sfo2.digitaloceanspaces.com/pagerank-417/Japanese_rock_music_groups.txt");
                topic_scanner = new BufferedReader(new InputStreamReader(topics.openStream())); 
               // topic_page_ids = new File("Japanese_rock_music_groups.txt");
               while ((line = topic_scanner.readLine()) != null) {
                    String[] data = line.split("\\s+");

                    for (String id: data) {
                        topic_pages_table.add(Integer.parseInt(id));
                    }
                    
                }
                topic_scanner.close();
            }catch(Exception e){
                e.printStackTrace();
            }
            }


            context.getCounter(KEEPALIVECOUNTER.ALIVE).increment(1);

            for (IntWritable value: values) {
                context.getCounter(KEEPALIVECOUNTER.ALIVE).increment(1);

                node = value.get();
                if (!(destinations.contains(node))) {
                    destinations.add(node);

                }

            }





            if (topic_pages_table.contains(key.get())) {
                inital_page_rank = new BigDecimal(0.85 * (1.0 / 1791489.0));
                inital_page_rank = inital_page_rank.add(teleport_divided);
                info_string = destinations.toString() + ":Topic:" + inital_page_rank.toPlainString();






            } else {
                inital_page_rank = new BigDecimal(0.85 * (1.0 / 1791489.0));
                info_string = destinations.toString() + "::" + inital_page_rank.toPlainString();

            }


            info_text.set(info_string);




            context.write(key, info_text);


        }
    }






    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int iterations = 10; //Number of iterations for the page rank algorithm. 
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1] + "initial"); //Quality of life add-on

        


           
            



        Job job = new Job(conf, "TopicPageRank");
        job.setJarByClass(TopicPageRank.class);
        job.setMapperClass(InitialTopicPageRankMapper.class);
        job.setReducerClass(InitialTopicPageRankReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
        inPath = outPath;




        for (int i = 0; i < iterations; ++i) {
            outPath = new Path(args[1] + i);
            Job job1 = new Job(conf, "TopicPageRank");
            job1.setJarByClass(TopicPageRank.class);
            job1.setMapperClass(TopicPageRankMapper.class);
            job1.setReducerClass(TopicPageRankReducer.class);
            job1.setOutputKeyClass(IntWritable.class);
            job1.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job1, inPath);
            FileOutputFormat.setOutputPath(job1, outPath);
            job1.waitForCompletion(true);
            inPath = outPath;
        }


        Job job2 = new Job(conf, "TopicPageRank");
        job2.setJarByClass(TopicPageRank.class);
        job2.setMapperClass(TopicCombinerMapper.class);
        job2.setReducerClass(TopicCombinerReducer.class);
        job2.setOutputKeyClass(RankIDPair.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setPartitionerClass(RankIDPartitioner.class);
        job2.setGroupingComparatorClass(RankIDGroupingComparator.class);
        FileInputFormat.addInputPath(job2, inPath);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"results")); //Quality of life add-on

        System.exit(job2.waitForCompletion(true) ? 0 : 1);





    }
}