package edu.nyu.bigdata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.StringTokenizer;

// hadoop dependencies
//  maven: org.apache.hadoop:hgadoop-client:x.y.z  (x..z = hadoop version, 3.3.0 here
import org.apache.commons.lang3.math.Fraction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

//This program will be 'started' by the Hadoop runtime

// This program takes the output produced by Q2 program as Input.
public class ProbabilityXGivenUnitedStates {
    // this is the driver
    public static void main(String[] args) throws Exception {
        // get a reference to a job runtime configuration for this program
        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: probabilityxgivenunitedstates <in> <out1> <out2>");
            System.exit(2);
        }

        // Set up Job 1, Output in format (key=>united_states, value=>x@probability(united states x) and
        // (key=>united_states_probability, value=>probability(united states)
        @SuppressWarnings("deprecation")
        Job job1 = new Job(conf1, "united states => x_probability");
        job1.setJarByClass(ProbabilityXGivenUnitedStates.class);
        job1.setMapperClass(USToXCountMapper.class);
        job1.setReducerClass(USToXCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        job1.waitForCompletion(true);

        // Set up Job 2, Output in format (key=>x, value=>p (x | united states))
        @SuppressWarnings("deprecation")
        Job job2 = new Job(conf2, "x => p (x | united states)");
        job2.setJarByClass(ProbabilityXGivenUnitedStates.class);
        job2.setMapperClass(XToConditionalProbabilityMapper.class);
        job2.setReducerClass(XToConditionalProbabilityReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    // this mapper maps united_states => x@probability(united states x) and
    // united_states_probability => probability(united states)
    public static class USToXCountMapper extends Mapper<Object, Text, Text, Text> {
        private static final Text x_prob = new Text();
        private static final Text newKey = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            // iterate over the next word tokens
            // As my output is of format x y z => probability
            // Hence maximum 5 tokens
            if (itr.countTokens() == 5) {           // trigram
                String united = itr.nextToken();    // first token
                String states = itr.nextToken();    // second token
                String x = itr.nextToken();         // third token
                itr.nextToken();                    // forth token
                String prob = itr.nextToken();      // fifth token
                if (united.equals("united") && states.equals("states")) { // trigram starts with united states
                    newKey.set(united+"_"+states);
                    x_prob.set(x+"@"+prob);
                    context.write(newKey, x_prob);      // (united states, x_probabiity(united states x))
                }
            } else if (itr.countTokens() == 4) {          // bigram
                String united = itr.nextToken();          // first token
                String states = itr.nextToken();          // second token
                itr.nextToken();                          // third token
                String prob = itr.nextToken();            // forth token
                if (united.equals("united") && states.equals("states")) {  // bigram is united states
                    newKey.set(united+"_"+states+"_probability");
                    context.write(newKey, new Text(prob));      // (united states probability, probabiity(united states))
                }
            }
        }
    }

    // this reducer maps p(x|united states) => x@probability
    public static class USToXCountReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            double maxProb = 0;
            Text newVal = new Text();
            Text newKey = new Text("p(x|united_states)");
            for (Text val: values) {
                if (key.toString().equals("united_states")) {
                    String[] x_prob = val.toString().split("@"); // get x at index 0 and probability at index 1
                    Fraction fraction = Fraction.getFraction(x_prob[1]);
                    double prob = fraction.doubleValue();
                    if (prob > maxProb) {
                        maxProb = prob;
                        newVal.set(x_prob[0]+"@"+x_prob[1]);
                    }
                } else {    // key is united states probability
                    String[] united_states_prob = key.toString().split("_");
                    String united_states = united_states_prob[0] + "_" + united_states_prob[1];
                    newVal.set(united_states+"@"+val);
                }
            }
            context.write(newKey, newVal);
        }
    }

    // this mapper maps p(x|united states) => x@probability
    public static class XToConditionalProbabilityMapper extends Mapper<Object, Text, Text, Text> {
        private static final Text newKey = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            // iterate over the next word tokens
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (token.equals("p(x|united_states)")) { // key to previous reducer
                    newKey.set(token);
                } else {
                    context.write(newKey, new Text(token));
                }
            }
        }
    }

    // this reducer outputs x with highest probability that follows united states
    public static class XToConditionalProbabilityReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Text x = new Text();
            String probability = "";
            for (Text val: values) {
                String[] x_prob = val.toString().split("@");
                if (x_prob[0].equals("united_states")) {
                    probability = probability + "(" + x_prob[1] + ")";
                } else {
                    x.set(x_prob[0]);
                    probability = "(" + x_prob[1] + ")/" + probability;
                }
            }
            context.write(x, new Text("\t => \t"+probability));
        }
    }
}

