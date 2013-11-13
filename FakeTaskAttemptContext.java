// Copyright (c) 2013 Aalto University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
//
// File created: 2013-02-06 11:04:15

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.security.Credentials;

import org.apache.hadoop.mapred.TaskAttemptID;

// A major hack to get around the fact that Hadoop-BAM adheres to the new API
// in that it takes TaskAttemptContexts, but only uses their Configurations.
public class FakeTaskAttemptContext implements TaskAttemptContext {
   private final Configuration conf;

   public FakeTaskAttemptContext(Configuration c) { conf = c; }

   @Override public Configuration getConfiguration() { return conf; }

   // All unused, but required.
   @Override public String getStatus() { return null; }
   @Override public void setStatus(String s) {}
   @Override public TaskAttemptID getTaskAttemptID() { return null; }
   @Override public int getMaxReduceAttempts() { return 0; }
   @Override public int getMaxMapAttempts() { return 0; }
   @Override public String[] getFileTimestamps() { return null; }
   @Override public String[] getArchiveTimestamps() { return null; }
   @Override public Path[] getFileClassPaths() { return null; }
   @Override public Path[] getLocalCacheFiles() { return null; }
   @Override public Path[] getLocalCacheArchives() { return null; }
   @Override public URI[] getCacheFiles() { return null; }
   @Override public URI[] getCacheArchives() { return null; }
   @Override public Path[] getArchiveClassPaths() { return null; }
   @Override public boolean getSymlink() { return false; }
   @Override public String getUser() { return null; }
   @Override public String getProfileParams() { return null; }
   @Override public boolean getProfileEnabled() { return false; }
   @Override public boolean getJobSetupCleanupNeeded() { return false; }
   @Override public RawComparator<?> getGroupingComparator() { return null; }
   @Override public String getJar() { return null; }
   @Override public RawComparator<?> getSortComparator() { return null; }
   @Override public Class<? extends Partitioner<?,?>> getPartitionerClass() {
      return null;
   }
   @Override public Class<? extends OutputFormat<?,?>> getOutputFormatClass() {
      return null;
   }
   @Override public Class<? extends InputFormat<?,?>> getInputFormatClass() {
      return null;
   }
   @Override public Class<? extends Reducer<?,?,?,?>> getReducerClass() {
      return null;
   }
   @Override public Class<? extends Reducer<?,?,?,?>> getCombinerClass() {
      return null;
   }
   @Override public Class<? extends Mapper<?,?,?,?>> getMapperClass() {
      return null;
   }
   @Override public boolean userClassesTakesPrecedence() { return false; }
   @Override public String getJobName() { return null; }
   @Override public Class<?> getMapOutputValueClass() { return null; }
   @Override public Class<?> getMapOutputKeyClass() { return null; }
   @Override public Class<?> getOutputValueClass() { return null; }
   @Override public Class<?> getOutputKeyClass() { return null; }
   @Override public Path getWorkingDirectory() { return null; }
   @Override public int getNumReduceTasks() { return 0; }
   @Override public JobID getJobID() { return null; }
   @Override public Credentials getCredentials() { return null; }
   @Override public void progress() {}
   @Override public Counter getCounter(String a, String b) { return null; }
   @Override public Counter getCounter(Enum<?> a) { return null; }
}
