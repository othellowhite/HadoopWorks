import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

// yoon // 150721 // 14:07 // helper function for parses the stackoveflow into a Map
import java.util.HashMap;
import java.util.Map;
// yoon // end

//
// Improved version of WordCount
//
public class WordCount2 {


  // yoon // 150721 // 14:11 // helper function for parses the stackoveflow into a Map
  public static Map<String, String> transformXmlToMap(String xml) {
    Map<String, String> map = new HashMap<String, String>();
    try {
      // exploit the fact that splitting on double quote
      // tokenizes the data nicely for us
      String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
      for (int i = 0; i < tokens.length - 1; i += 2) {
        String key = tokens[i].trim();
        String val = tokens[i + 1];
        map.put(key.substring(0, key.length() - 1), val);
      }
    } catch (StringIndexOutOfBoundsException e) {
      System.err.println(xml);
    }
    return map;
  } // yoon // end
  
  
  public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
    //private final static LongWritable one = new LongWritable(1);
    private Text word = new Text();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      
      // yoon // parse the ipt string into a map [MRDP, p.9]
      Map<String, String> parsed = transformXmlToMap(value.toString());
      
      // yoon // 14:59 // Grab field [MRDP, p.9]
      String location = parsed.get("Location");
      String ageStr = parsed.get("Age");
      if (location == null || ageStr == null) { // skip this record
        return; 
      } else {
        LongWritable age = new LongWritable(Integer.parseInt(ageStr));
        word.set(location);
        context.write(word, age);
      }
    }
  }

  public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable sumWritable = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long sum = 0;
      long count = 0; // yoon // 03:16 // counting for average
      for (LongWritable val : values) {
        sum += val.get();
        count++;
      }
      sumWritable.set(sum/count);
      context.write(key, sumWritable);
      context.getCounter("Words Stats", "Unique Words").increment(1);
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "WordCount2");

    job.setJarByClass(WordCount2.class);
    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // if mapper outputs are different, call setMapOutputKeyClass and setMapOutputValueClass
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // An InputFormat for plain text files. Files are broken into lines. Either linefeed or
    // carriage-return are used to signal end of line.
    // Keys are the position in the file, and values are the line of text..
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }

}
