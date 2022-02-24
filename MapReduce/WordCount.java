// dit schrijft onderstaande cell naar een file in de huidige directory
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    // Mapper de classes na extends Mapper<> zijn input-key, input-value, output-key, output-value
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // split de tekst via spaties/newlines -> itereren over woorden
            // hello world, ik ben jens
            StringTokenizer itr = new StringTokenizer(value.toString());
            // splitsen in 
            // hello
            //world,
            //ik
            //ben
            //jens
            while(itr.hasMoreTokens()){
                // next token = vraag het volgende woord op
                word.set(itr.nextToken());
                context.write(word, one);
                // (hello, 1)
                // (world, 1)
                // ...
            }
        }
    }

    // Reducer: de classes na extends Reducer<> zijn input-key, input-value, output-key, output-value
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val: values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // configure the MapReduce program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count java");
        // waar staat de map en reduce code
        job.setJarByClass(WordCount.class);
        
        // configure mapper
        job.setMapperClass(TokenizerMapper.class);
        
        //configure reducer
        job.setReducerClass(IntSumReducer.class);
        
        // configure input/output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // input file
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // output file
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // start de applicatie
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
