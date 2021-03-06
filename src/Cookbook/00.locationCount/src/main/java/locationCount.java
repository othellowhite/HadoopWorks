import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringEscapeUtils;


public class locationCount extends Mapper<Object, Text, Text, IntWritable> {


  // This helper function parses the stackoverflow into a Map for us.
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
  }


  public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {


      // Parse the input string into a nice map
      Map<String, String> parsed = transformXmlToMap(value.toString());

      // Grab the "Text" field, since that is what we are counting over
      String txt = parsed.get("Location");
      // .get will return null if the key is not there
      if (txt == null) { // skip this record
        return;
      }

      // Unescape the HTML because the data is escaped.
      txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());   
      
      // Remove some annoying punctuation
      txt = txt.replaceAll("'", "");           // remove single quotes (e.g., can't) 
      txt = txt.replaceAll("[^a-zA-Z]", " ");  // replace the rest with a space

      word.set(txt);
      context.write(word, one);
      /*

      // Tokenize the string by splitting it up on whitespace into // something we can iterate over,
      // then send the tokens away
      StringTokenizer itr = new StringTokenizer(txt);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }*/
    }
  }


  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if (sum>=10) {
        result.set(sum);
        context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: LocatioCnount <in> <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "locationCount");
    job.setJarByClass(locationCount.class);
    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
