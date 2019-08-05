import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;


public class MutualFriends {

    //Mapper Class
    public static class FMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text w = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] ip = value.toString().split("-"); //Splitting the input "-"
            if (ip.length == 2) {
                String friend1 = ip[0]; //This line is written to get the usernames of people
                List<String> values = Arrays.asList(ip[1].split(",")); //Splitting of friends based on ','
                for (String friend2 : values)
                {
                    //Getting each friend's value and grouping

                    if (Integer.parseInt(friend1) < Integer.parseInt(friend2))
                        w.set(friend1 + "," + friend2); //Setting word as mapping output.
                    else
                        w.set(friend2 + "," + friend1);
                    context.write(w, new Text(ip[1]));
                }
            }
        }

    }
    public static class FReducer extends Reducer<Text, Text, Text, Text>
    {

        private Text res = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            HashMap<String, Integer> FMap = new HashMap<String, Integer>();
            StringBuilder sb = new StringBuilder();
            for (Text friends : values)
            {
                List<String> temporary = Arrays.asList(friends.toString().split(","));
                for (String mutual : temporary)
                {
                    if (FMap.containsKey(mutual))
                        sb.append(mutual + ',');
                    else
                        FMap.put(mutual, 1);

                }
            }

            res.set(new Text(sb.toString()));
            context.write(key, res); //Generating the reduced output.
        }
    }


    //This is a driver Class
    public static void main(String[] args) throws Exception
    {
        if (args.length != 2)
        {
            System.err.println("ERROR! INSUFFICIENT NUMBER OF ARGUMENTS"); //If the number of program arguments passed are less than two, Print this statement.
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf, "MutualFriends");
        job.setJarByClass(MFriends.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(FMapper.class);
        job.setReducerClass(FReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); //Input file
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //Output file

        job.waitForCompletion(true);

    }


}
