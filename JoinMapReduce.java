import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.contrib.utils.join.*; 

public class JoinMapReduce extends Configured implements Tool 
    {
       
        public static class Reducer extends DataJoinReducerBase
            {
                //Join takes place here, by combine() function
                protected TaggedMapOutput combine(Object[] join_tags, Object[] values) 
                    {
                		int i=0;
                        if (join_tags.length < 2) 
                        	return null;
                        String result_string = "";
                        while (i<values.length) 
                        {
                            if (i > 0) 
                            	result_string += ",";
                            FlagInput tw = (FlagInput) values[i];
                            String stmt = ((Text) tw.getData()).toString();
                            String[] flag = stmt.split(",", 2);
                            result_string += flag[1];
                            i++;
                        }
                        FlagInput tw = new FlagInput(new Text(result_string));
                        tw.setTag((Text) join_tags[0]);
                        return tw;
                    }
            }
        //This function begins the job execution, using the default configuration
        public int execute(String[] args) throws Exception 
        {


                            Configuration conf = getConf();
                            JobConf job = new JobConf(conf, JoinMapReduce.class);
                            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                            if (otherArgs.length != 2) 
                            {
                              System.err.println("Wrong Argument Count");
                              System.exit(2);
                            }

            Path input = new Path(args[0]);
            Path output = new Path(args[1]);
            FileInputFormat.setInputPaths(job, input);
            FileOutputFormat.setOutputPath(job, output);
            job.setJobName("JoinMapReduce");
            job.setInputFormat(TextInputFormat.class);
            job.setOutputFormat(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlagInput.class);
            job.setMapperClass(MapClass.class);
            job.setReducerClass(Reducer.class);
            job.set("mapred.textoutputformat.separator", ",");
            JobClient.runJob(job);
            return 0;
        }
        //Specifies default and overridden constructors for FlagInput
        public static class FlagInput extends TaggedMapOutput {
    private Writable data;

    public FlagInput() {
        this.tag = new Text();
    }

    public FlagInput(Writable value) {
        this.tag = new Text("");
        this.data = value;
    }

    public Writable getData() {
        return data;
    }

    public void setData(Writable data) {
        this.data = data;
    }

    public void write(DataOutput result) throws IOException {
        this.tag.write(result);
        result.writeUTF(this.data.getClass().getName());
        this.data.write(result);
    }

    public void fetch(DataInput di) throws IOException {
        this.tag.readFields(di);
        String class_data = di.readUTF();
        if (this.data == null|| !this.data.getClass().getName().equals(class_data)) {
            this.data = (Writable) ReflectionUtils.newInstance(
                    Class.forName(class_data), null);
        }
        this.data.readFields(in);
    }
}
        public static class MapClass extends DataJoinMapperBase
        {
            protected Text InputTagGeneration(String input) 
                {
                    String getdata = input.split("-")[0];
                    return new Text(getdata);
                }
        
        protected MapOutput generateTaggedMapOutput(Object v) 
            {
                FlagInput tw = new FlagInput((Text) v);
                tw.setTag(this.inputTag);
                return tw;
            }
        protected Text generateGroupKey(TaggedMapOutput storage_recd) 
        {
            String sntc = ((Text) storage_recd.getData()).toString();
            String[] flag = sntc.split(",");
            String Key = flag[0];
            return new Text(Key);
        }
        }
        
        public static void main(String[] args) throws Exception 
            {
                int res = ToolRunner.run(new Configuration(),new JoinMapReduce(),args);
                System.exit(res);
            }
    }