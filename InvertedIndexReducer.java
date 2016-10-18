
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(final Text key, final Iterable<Text> values, final Context context)
			throws IOException, InterruptedException {

		// <keyword, <doc1, doc1, doc1, doc2, doc2, doc2, doc2>>
		// <keyword, doc1\tdoc2>
		int threashold = 100; 
		StringBuilder sb = new StringBuilder();
		String lastBook = null;
		int count = 0; //当前关键词在同一篇文章中出现的次数

		for (Text value : values) {
			//如果当前文章名与前一个文章名相同，则这个关键词出现的count+1
			if (lastBook != null && value.toString().trim().equals(lastBook)) {
				count++;
				continue;
			}

			// lastbook = null OR
			// value != lastBook

			//如果当前文章名与前一个文章名不同，则表示上一篇文章的统计完成
			//根据count得知关键词在上一篇文章中出现的次数，如果次数小于threashold，则不放入最终结果
			if (lastBook != null & count < threashold) {
				count = 1;
				lastBook = value.toString().trim();
				continue;
			}

			//如果lastbook==null则要进行初始化，设置lastbook=当前value，并且当前value已经出现一次，所以count++
			if (lastBook == null) {
				lastBook = value.toString().trim();
				count++;
				continue;
			}

			//将bookname录入最终统计结果
			sb.append(lastBook);
			sb.append("\t");

			count = 1;
			lastBook = value.toString().trim();
		}

		//将values<>当中的最后一个bookname进行判断
		if (count >= threashold) {
			sb.append(lastBook);
		}

		//如果sb（bookname）不为空，则写出到最终结果
		if (!sb.toString().trim().equals("")) {
			context.write(key, new Text(sb.toString()));
		}

	}

}
