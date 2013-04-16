// File created: 2013-02-15 09:52:43

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapred.RecordReader;

import net.sf.samtools.SAMRecord;

import fi.tkk.ics.hadoop.bam.SAMRecordWritable;

public class DeprecatedBAMBaseRecordReader
   implements RecordReader<LongWritable,SAMBaseRecord>
{
   private final RecordReader<LongWritable,SAMRecordWritable> samRR;

   private SAMRecordWritable current;

   public DeprecatedBAMBaseRecordReader(
      RecordReader<LongWritable,SAMRecordWritable> rr)
   {
      samRR = rr;
   }

   @Override public void close() throws IOException { samRR.close(); }
   @Override public LongWritable  createKey  () { return new LongWritable (); }
   @Override public SAMBaseRecord createValue() { return new SAMBaseRecord(); }
   @Override public long getPos() throws IOException { return samRR.getPos(); }
   @Override public float getProgress() throws IOException {
      return samRR.getProgress();
   }

   @Override public boolean next(LongWritable key, SAMBaseRecord val)
      throws IOException
   {
      if (current == null)
         current = samRR.createValue();
      else if (nextFromOldCurrent(key, val))
         return true;
      do
         if (!samRR.next(key, current))
            return false;
      while (!nextFromNewCurrent(key, val));
      return true;
   }

   private boolean nextFromNewCurrent(LongWritable key, SAMBaseRecord base) {
      final SAMRecord sam = current.get();

      if (sam.getReadUnmappedFlag()
       || sam.getAlignmentStart() == 0
       || sam.getReadName().equals("*"))
      {
         // CIGAR is unusable.
         return false;
      }

      base.setParent(current);
      return true;
   }

   private boolean nextFromOldCurrent(LongWritable key, SAMBaseRecord base) {
      assert base.getParent() == current;

      if (!base.gotoNextBase())
         return false;

      key.set((key.get() & ~0xffffffffL) | base.getPos());
      return true;
   }
}
