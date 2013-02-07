// File created: 2013-02-05 15:14:39

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import fi.tkk.ics.hadoop.bam.BAMInputFormat;
import fi.tkk.ics.hadoop.bam.FileVirtualSplit;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;

// Wraps BAMInputFormat, providing the deprecated mapred API.
public class DeprecatedBAMInputFormat
   extends FileInputFormat<LongWritable,SAMRecordWritable>
{
   @Override public RecordReader<LongWritable,SAMRecordWritable>
         getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException
   {
      return new DeprecatedBAMRecordReader(split, job, reporter);
   }

   @Override public InputSplit[] getSplits(JobConf job, int numSplits)
      throws IOException
   {
      return deprecateSplits(new BAMInputFormat().getSplits(
         undeprecateSplits(super.getSplits(job, numSplits)), job));
   }

   public static DeprecatedFileVirtualSplit[] deprecateSplits(
      List<org.apache.hadoop.mapreduce.InputSplit> splits)
   {
      final DeprecatedFileVirtualSplit[] deprecated =
         new DeprecatedFileVirtualSplit[splits.size()];
      for (int i = 0; i < splits.size(); ++i)
         deprecated[i] =
            new DeprecatedFileVirtualSplit((FileVirtualSplit)splits.get(i));
      return deprecated;
   }
   public static List<org.apache.hadoop.mapreduce.InputSplit>
         undeprecateSplits(InputSplit[] splits)
      throws IOException
   {
      final List<org.apache.hadoop.mapreduce.InputSplit> undeprecated =
         new ArrayList<org.apache.hadoop.mapreduce.InputSplit>(splits.length);
      for (final InputSplit s : splits) {
         final FileSplit f = (FileSplit)s;
         undeprecated.add(
            new org.apache.hadoop.mapreduce.lib.input.FileSplit(
               f.getPath(), f.getStart(), f.getLength(), f.getLocations()));
      }
      return undeprecated;
   }

   @Override public boolean isSplitable(FileSystem fs, Path path) {
      return true;
   }
}
