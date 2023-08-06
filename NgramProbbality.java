// namespace
package edu.nyu.bigdata;

//java dependencioes
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.StringTokenizer;

// hadoop dependencies
//  maven: org.apache.hadoop:hgadoop-client:x.y.z  (x..z = hadoop version, 3.3.0 here
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

//our program. This program will be 'started' by the Hadoop runtime
public class NGramProbability {
    // this is the driver
    public static void main(String[] args) throws Exception {
        // get a reference to a job runtime configuration for this program
        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();
        Configuration conf3 = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: ngramprobability <in> <out1> <out2> <out3>");
            System.exit(2);
        }

        // Set up Job 1, Output in format (key=>ngarm type#, value=>tokens@count)
        @SuppressWarnings("deprecation")
        Job job1 = new Job(conf1, "ngram type# => tokens@count");
        job1.setJarByClass(NGramProbability.class);
        job1.setMapperClass(NgramTypeTokenCountMapper.class);
        job1.setReducerClass(NgramTypeTokenCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        job1.waitForCompletion(true);

        // Set up Job 2 Output in format (key=>ngram count#, value=>tokens@tokenCount)
        @SuppressWarnings("deprecation")
        Job job2 = new Job(conf2, "ngram count# => tokens@count");
        job2.setJarByClass(NGramProbability.class);
        job2.setMapperClass(NgramCountTokenCountMapper.class);
        job2.setReducerClass(NgramCountTokenCountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
        job2.waitForCompletion(true);

        // Set up Job 3 Output in format (key=>token, value=>probability)
        @SuppressWarnings("deprecation")
        Job job3 = new Job(conf3, "ngram token => ngram probability");
        job3.setJarByClass(NGramProbability.class);
        job3.setMapperClass(ResultMapper.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }

    // 1) removes all characters not in set [a-z, A-Z, 0-9]
    // 2) turns all text to lower case
    public static String cleanInput(String word) {
        return word.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
    }

    // returns the first valid word from the current token passed
    public static String getWord(StringTokenizer itr) {
        String word = "";
        while (itr.hasMoreTokens() && word.equals("")) {
            word = NGramProbability.cleanInput(itr.nextToken());
        }
        return word;
    }

    // this mapper maps token => count
    public static class NgramTypeTokenCountMapper extends Mapper<Object, Text, Text, Text> {
        private static final Text word = new Text();
        private static final Text ngram = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            // get the first valid word of the line and write to context
            String first = NGramProbability.getWord(itr);
            word.set(first);
            ngram.set("1#");
            context.write(word, ngram);

            // get the second valid word of the line and write to context
            String second = NGramProbability.getWord(itr);
            word.set(second);
            ngram.set("1#");
            context.write(word, ngram);

            // write the first bigram to context
            String bigram = first + " " + second;
            word.set(bigram);
            ngram.set("2#");
            context.write(word, ngram);

            // iterate over the next word tokens
            while (itr.hasMoreTokens()) {
                String third = NGramProbability.cleanInput(itr.nextToken());
                // check if the word is valid
                if (!third.equals("")) {
                    // write the unigram to context
                    word.set(third);
                    ngram.set("1#");
                    context.write(word, ngram);

                    // write the bigram to context
                    bigram = second + " " + third;
                    word.set(bigram);
                    ngram.set("2#");
                    context.write(word, ngram);

                    // write the trigram to context
                    String trigram = first + " " + second + " " + third;
                    word.set(trigram);
                    ngram.set("3#");
                    context.write(word, ngram);

                    // update the previous two words
                    first = second;
                    second = third;
                }
            }
        }
    }

    // this reducer maps ngram type => list of tokens@tokenCount
    public static class NgramTypeTokenCountReducer extends Reducer<Text,Text,Text,Text> {
        private static final Text result = new Text();
        private static final Text newKey = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int count = 0;
            for (Text val: values) {
                if (count == 0) {
                    newKey.set(val);
                }
                count += 1;
            }
            result.set(key+"@"+count);
            context.write(newKey, result);
        }
    }

    // this class implements the ngram type => ngram tokens mapper
    public static class NgramCountTokenCountMapper extends Mapper<Object, Text, Text, Text> {
        private static final Text word = new Text();
        private static final Text newKey = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(),"#");
            newKey.set(itr.nextToken().trim()+"#");
            word.set(itr.nextToken().trim());
            context.write(newKey, word);
        }
    }

    // this class implements the ngram count => ngram tokens reducer
    public static class NgramCountTokenCountReducer extends Reducer<Text,Text,Text,Text> {
        private static final Text newKey = new Text();
        private static final Text newValue = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int total = 0;
            StringBuilder tokens = new StringBuilder();
            for (Text val : values) {
                tokens.append(val).append("$");
                total += 1;
            }
            newKey.set(total+"#");
            newValue.set(tokens.toString());
            context.write(newKey, newValue);
        }
    }

    // this class implements the ngram token => ngram probability mapper
    public static class ResultMapper extends Mapper<Object, Text, Text, Text> {
        private static final Text word = new Text();
        private static final Text newKey = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(),"#");
            String total = itr.nextToken().trim();
            String[] tokens = itr.nextToken().split("\\$");
            for (int i=0; i<Integer.parseInt(total); i++) {
                String[] tokenCount = tokens[i].split("@");
                newKey.set(tokenCount[0].trim() + "\t => \t");
                word.set(tokenCount[1]+"/"+total);
                context.write(newKey, word);
            }
        }
    }
}


// Sample i/p and o/p
/*
I/p => Cat bat cat dog cat.

Job 1 O/p:
1#	bat@1
2#	bat cat@1
3#	bat cat dog@1
1#	cat@3
2#	cat bat@1
3#	cat bat cat@1
2#	cat dog@1
3#	cat dog cat@1
1#	dog@1
2#	dog cat@1

Job 2 O/p:
3#	dog@1$cat@3$bat@1$
4#	dog cat@1$cat dog@1$cat bat@1$bat cat@1$
3#	cat dog cat@1$cat bat cat@1$bat cat dog@1$

Job 3 O/p
bat	1/3
bat cat	1/4
bat cat dog	1/3
cat	3/3
cat bat	1/4
cat bat cat	1/3
cat dog	1/4
cat dog cat	1/3
dog	1/3
dog cat	1/4
 */
