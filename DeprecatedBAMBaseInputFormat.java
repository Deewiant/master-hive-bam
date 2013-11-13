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
// File created: 2013-02-15 09:43:07

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class DeprecatedBAMBaseInputFormat
   extends FileInputFormat<LongWritable,SAMBaseRecord>
{
   private final DeprecatedBAMInputFormat bamInputFormat =
      new DeprecatedBAMInputFormat();

   @Override public RecordReader<LongWritable,SAMBaseRecord>
         getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException
   {
      return new DeprecatedBAMBaseRecordReader(
         bamInputFormat.getRecordReader(split, job, reporter));
   }

   @Override public InputSplit[] getSplits(JobConf job, int numSplits)
      throws IOException
   {
      return bamInputFormat.getSplits(job, numSplits);
   }
}
