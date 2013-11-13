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
