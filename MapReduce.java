import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

import javax.naming.Context;

public class MapReduce {

    public static class IMCMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text gender = new Text();
        private DoubleWritable imc = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            if (fields[0].equals("id") || fields[0].equals("\"id\"")) {
                return;
            }
            if (fields.length == 5) {
                String sex = fields[4].replaceAll("\"", "").trim();
                double height = Double.parseDouble(fields[2])/100;
                double weight = Double.parseDouble(fields[3]);
                int age = Integer.parseInt(fields[1]);

                if (age > 17) {
                    double imcValue = calculateIMC(height, weight);

                    gender.set(sex);
                    imc.set(imcValue);

                    context.write(gender, imc);
                }
            }
        }

        private double calculateIMC(double height, double weight) {
            return weight / (Math.pow(height, 2));
        }
    }

    public static class IMCReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            int count = 0;
            ArrayList<Double> imcs = new ArrayList<>();

            for (DoubleWritable value : values) {
                double imc = value.get();
                imcs.add(imc);
                sum += imc;
                min = Math.min(min, imc);
                max = Math.max(max, imc);
                count ++;
            }

            double average = sum / count;
            double varianceSum = 0;

            for (Double imc : imcs) {;
                varianceSum += Math.pow(imc - average, 2);
            }

            double stdDev = Math.sqrt(varianceSum / count);

            String result = String.format("µ: %.2f, σ: %.2f, Min: %.2f, Max: %.2f", average, stdDev, min, max);
    
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IMC MapReduce");

        job.setJarByClass(MapReduce.class);
        job.setMapperClass(IMCMapper.class);
        job.setReducerClass(IMCReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}