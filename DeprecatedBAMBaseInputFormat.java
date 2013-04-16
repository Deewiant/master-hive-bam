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
