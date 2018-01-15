
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class MaxTemperatureReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce (Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException{
        int maxTemp = Integer.MIN_VALUE;
        for(IntWritable value : values) {
            if(value.get() > maxTemp) {
                maxTemp = value.get();
            }
        }
        context.write(key, new IntWritable(maxTemp));
    }
}
