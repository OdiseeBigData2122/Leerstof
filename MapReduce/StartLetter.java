import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StartLetter {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // split de tekst via spaties/newlines -> itereren over woorden
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            while(itr.hasMoreTokens()){
                // next token = vraag het volgende woord op
                String woord = itr.nextToken();
                // zet het om naar lowercase
                woord = woord.toLowerCase();
                // eerste letter eruit halen
                char firstLetter = woord.charAt(0);
                
                // negeer leestekens, cijfers, etc.
                if(Character.isLetter(firstLetter)){
                    word.set(firstLetter);    
                    context.write(word, one);    
                }
            }
        }
    }

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

    public static void main(String[] args) throws Exception {
       
    }
}
