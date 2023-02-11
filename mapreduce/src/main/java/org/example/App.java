package org.example;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

/*
 *  Суммарный объем торгов
 *
 *  Запуск:
 *      hadoop jar mapreduce-1.0-jar-with-dependencies.jar org.example.App /bronze /gold/result
 */
public class App extends Configured implements Tool {
    public static class MapClass extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, LongWritable> {

        private Text symbol = new Text();
        private LongWritable volume = new LongWritable();

        public void map(LongWritable key,
                        Text value,
                        OutputCollector<Text, LongWritable> output,
                        Reporter reporter) throws IOException {

            if (key.equals(new LongWritable(0))) return;

//            System.out.println("key: " + key + ", value: " + value);

            String line = value.toString();
            CSVReader reader = new CSVReader(new StringReader(line));

            String[] parsedLine;
            try {
                parsedLine = reader.readNext();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            reader.close();

            if (parsedLine.length == 7) {
                symbol.set(parsedLine[6]);
                volume.set(new LongWritable(Long.parseLong(parsedLine[5])).get());
            } else {
                symbol.set("unknown");
                volume.set(new LongWritable(0).get());
            }

            output.collect(symbol, volume);
        }
    }

    public static class Reduce extends MapReduceBase
            implements Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key,
                           Iterator<LongWritable> values,
                           OutputCollector<Text, LongWritable> output,
                           Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                LongWritable x = values.next();
                sum += x.get();
            }
            output.collect(key, new LongWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        JobConf job = new JobConf(conf, App.class);

        Path in = new Path(args[1]);
        Path out = new Path(args[2]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("MyJob");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        JobClient.runJob(job);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(res);
    }
}
