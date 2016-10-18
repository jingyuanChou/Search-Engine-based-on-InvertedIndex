
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	List<String> stopWords = new ArrayList<String>();

	@Override
	protected void setup(Context context) throws IOException {

		Configuration conf = context.getConfiguration(); // -->/stopWords/stopWords.txt
		String filePath = conf.get("filePath");

		Path pt = new Path(filePath);// Location of file in HDFS
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line;
		line = br.readLine();
		//read stopWords.txt line by line
		while (line != null) {
			stopWords.add(line.trim().toLowerCase());
			line = br.readLine();
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//get file name
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		Text name = new Text(fileName);

		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		while (tokenizer.hasMoreTokens()) {
			String curWord = tokenizer.nextToken().toString().toLowerCase();
			//replace non-alphabetic characters
			curWord = curWord.replaceAll("[^a-zA-Z]", ""); // replace(a, b)

			if (!stopWords.contains(curWord)) {
				context.write(new Text(curWord), name);
			}
		}
	}
}
