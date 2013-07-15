// File created: 2013-02-05 15:35:55

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import net.sf.samtools.seekablestream.SeekableStream;

import fi.tkk.ics.hadoop.bam.BAMRecordReader;
import fi.tkk.ics.hadoop.bam.BAMSplitGuesser;
import fi.tkk.ics.hadoop.bam.FileVirtualSplit;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.util.WrapSeekable;

// Wraps BAMRecordReader, providing the deprecated mapred API.
public class DeprecatedBAMRecordReader
   implements RecordReader<LongWritable,SAMRecordWritable>
{
   private final BAMRecordReader rr = new BAMRecordReader();

   private final long splitLength;

   public DeprecatedBAMRecordReader(
         InputSplit split, final JobConf job, Reporter reporter)
      throws IOException
   {
      if (split instanceof DeprecatedFileVirtualSplit) {
         rr.initialize(
            ((DeprecatedFileVirtualSplit)split).vs,
            new FakeTaskAttemptContext(job));

         splitLength = split.getLength();
         return;

      }
      if (split instanceof FileSplit) {
         // XXX             XXX
         //     XXX     XXX
         //         XXX
         //     XXX     XXX
         // XXX             XXX
         //
         // Hive gives us its own custom FileSplits for some reason, so we have
         // to do our own split alignment. (Sometimes, anyway; for "select
         // count(*) from table" we get FileSplits here, but for "select * from
         // table" our input format is used directly. Perhaps it's only because
         // the latter doesn't spawn a MapReduce job, so getting a FileSplit
         // here is the common case.)
         //
         // Since we get only one split at a time here, this is very poor: we
         // have to open the file for every split, even if it's the same file
         // every time.
         //
         // This should always work, but might be /very/ slow. I can't think of
         // a better way.

         final FileSplit fspl = (FileSplit)split;
         final Path path = fspl.getPath();

         final long beg =       fspl.getStart();
         final long end = beg + fspl.getLength();

         final SeekableStream sin =
            WrapSeekable.openPath(path.getFileSystem(job), path);
         final BAMSplitGuesser guesser = new BAMSplitGuesser(sin);

         final long alignedBeg = guesser.guessNextBAMRecordStart(beg, end);
         sin.close();

         if (alignedBeg == end)
            throw new IOException("Guesser found nothing after pos " +beg);

         final long alignedEnd = end << 16 | 0xffff;
         splitLength = (alignedEnd - alignedBeg) >> 16;

         rr.initialize(
            new FileVirtualSplit(
               path, alignedBeg, alignedEnd, fspl.getLocations()),
            new FakeTaskAttemptContext(job));
         return;
      }

      throw new ClassCastException(
         "Can only handle DeprecatedFileVirtualSplit and FileSplit");
   }

   @Override public void close() throws IOException { rr.close(); }
   @Override public LongWritable createKey() { return new LongWritable(); }
   @Override public SAMRecordWritable createValue() {
      return new SAMRecordWritable();
   }
   @Override public long getPos() {
      return splitLength == 0 ? 1 : (long)(getProgress() * splitLength);
   }
   @Override public float getProgress() { return rr.getProgress(); }
   @Override public boolean next(LongWritable key, SAMRecordWritable value) {
      if (!rr.nextKeyValue())
         return false;
      key.set(rr.getCurrentKey().get());
      value.set(rr.getCurrentValue().get());
      return true;
   }
}
