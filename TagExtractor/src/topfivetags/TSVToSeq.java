package topfivetags;

/*
 * Copyright (c) 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

/**
 * http://www.chimpler.com
 */
public class TSVToSeq {
	public static void main(String args[]) throws Exception {
		String inputFileName = "result/output-topfivetags/part-r-00000.tsv";
		String outputDirName = "result/output-tsvtoseq";
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		Writer writer = new SequenceFile.Writer(fs, configuration, new Path(outputDirName + "/chunk-0"),
				Text.class, Text.class);
		
		int count = 0;
		BufferedReader reader = new BufferedReader(new FileReader(inputFileName));
		Text key = new Text();
		Text value = new Text();
		while(true) {
			// for each question
			String line = reader.readLine();
			if (line == null) {
				break;
			}
			String[] tokens = line.split("\t");
			if (tokens.length != 4) {
				System.out.println("Skip line: " + line);
				continue;
			}
			String category = tokens[3];
			String id = tokens[0];
			String title = tokens[1];
			String body = tokens[2];
			String message = title + " " + body; // combine title and body
			key.set("/" + category + "/" + id);
			value.set(message);
			writer.append(key, value); // write to sequence file with given key and value
			count++;
		}
		reader.close();
		writer.close();
		System.out.println("Wrote " + count + " entries.");
	}
}
