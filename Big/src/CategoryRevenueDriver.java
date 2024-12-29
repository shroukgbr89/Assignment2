import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

// File: CategoryRevenueDriver.java
public class CategoryRevenueDriver {

    // Mapper: CategoryRevenueMapper
    public static class CategoryRevenueMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text category = new Text();
        private DoubleWritable revenue = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // Ensure there are enough fields in the CSV line
            if (fields.length >= 6) {
                String productCategory = fields[3];  // Assume fields[3] is the category
                try {
                    // Check if category is not empty and if price and quantity can be parsed as numbers
                    if (productCategory != null && !productCategory.trim().isEmpty()) {
                        double price = Double.parseDouble(fields[5]);  // Assume fields[5] is the price
                        int quantity = Integer.parseInt(fields[4]);    // Assume fields[4] is the quantity

                        // Calculate revenue and write it with the category as the key
                        revenue.set(price * quantity);
                        category.set(productCategory);
                        context.write(category, revenue);
                    }
                } catch (NumberFormatException e) {
                    // Skip invalid records
                }
            }
        }
    }

    // Combiner: CategoryRevenueCombiner
    public static class CategoryRevenueCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable partialRevenue = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            partialRevenue.set(sum);
            context.write(key, partialRevenue);
        }
    }

    // Reducer: CategoryRevenueReducer
    public static class CategoryRevenueReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable totalRevenue = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }

            // Set the total revenue and write the result
            totalRevenue.set(sum);
            context.write(key, totalRevenue);
        }
    }

    // Driver: main method
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Revenue Calculation");
        job.setJarByClass(CategoryRevenueDriver.class);

        // Set Mapper, Combiner, and Reducer classes
        job.setMapperClass(CategoryRevenueMapper.class);
        job.setCombinerClass(CategoryRevenueCombiner.class);  // Set the Combiner
        job.setReducerClass(CategoryRevenueReducer.class);

        // Set the types of output keys and values for the Map and Reduce phases
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set the input and output paths for the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit the job with appropriate status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
