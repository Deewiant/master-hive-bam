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
// File created: 2013-02-05 15:14:48

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;

import net.sf.picard.sam.SamFileHeaderMerger;

import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;

import fi.tkk.ics.hadoop.bam.KeyIgnoringBAMOutputFormat;
import fi.tkk.ics.hadoop.bam.KeyIgnoringBAMRecordWriter;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;

// Wraps KeyIgnoringBAMOutputFormat, providing Hive's custom RecordWriter API.
public class HiveKeyIgnoringBAMOutputFormat
   extends    FileOutputFormat<Writable,SAMRecordWritable>
   implements HiveOutputFormat<Writable,SAMRecordWritable>
{
   private KeyIgnoringBAMOutputFormat<Writable> wrappedOutputFormat =
      new KeyIgnoringBAMOutputFormat<Writable>();

   private void setSAMHeaderFrom(JobConf job) throws IOException {
      if (wrappedOutputFormat.getSAMHeader() != null)
         return;

      // XXX: We're not told where to take the SAM header from so we just merge
      // them all. There should probably be a better way of doing this.

      final List<SAMFileHeader> headers = new ArrayList<SAMFileHeader>();

      // The "best" sort order among the headers: unsorted if they're sorted
      // differently, otherwise their common sort order.
      SAMFileHeader.SortOrder sortOrder = null;

      // XXX: it seems that FileInputFormat.getInputPaths(job) will point to
      // the directories of the input tables in the query. I'm not sure if this
      // is always the case.
      for (final Path table : FileInputFormat.getInputPaths(job)) {
         final FileSystem fs = table.getFileSystem(job);
         for (final FileStatus stat : fs.listStatus(table)) {
            if (!stat.isFile())
               throw new IOException(
                  "Unexpected directory '" + stat.getPath() +
                  "', expected only files");

            final SAMFileReader r = new SAMFileReader(fs.open(stat.getPath()));
            final SAMFileHeader h = r.getFileHeader();
            r.close();
            headers.add(h);

            if (sortOrder == null) {
               sortOrder = h.getSortOrder();
               continue;
            }
            if (sortOrder == SAMFileHeader.SortOrder.unsorted)
               continue;
            if (sortOrder != h.getSortOrder())
               sortOrder = SAMFileHeader.SortOrder.unsorted;
         }
      }

      wrappedOutputFormat.setSAMHeader(
         new SamFileHeaderMerger(sortOrder, headers, true).getMergedHeader());
   }

   @Override public FileSinkOperator.RecordWriter getHiveRecordWriter(
         JobConf job, Path finalOutPath,
         final Class<? extends Writable> valueClass, boolean isCompressed,
         Properties tableProperties, Progressable progress)
      throws IOException
   {
      setSAMHeaderFrom(job);

      final FakeTaskAttemptContext ctx = new FakeTaskAttemptContext(job);

      final org.apache.hadoop.mapreduce.RecordWriter
               <Writable,SAMRecordWritable>
         wrappedRecordWriter =
            wrappedOutputFormat.getRecordWriter(ctx, finalOutPath);

      return new FileSinkOperator.RecordWriter() {
         @Override public void write(Writable rec) throws IOException {
            try {
               wrappedRecordWriter.write(null, (SAMRecordWritable)rec);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }
         @Override public void close(boolean abort) throws IOException {
            try {
               wrappedRecordWriter.close(ctx);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }
      };
   }

   @Override public RecordWriter<Writable,SAMRecordWritable> getRecordWriter(
         FileSystem fs, JobConf job, String name, Progressable progress)
      throws IOException
   {
      setSAMHeaderFrom(job);

      final FakeTaskAttemptContext ctx = new FakeTaskAttemptContext(job);

      final org.apache.hadoop.mapreduce.RecordWriter
               <Writable,SAMRecordWritable>
         wrappedRecordWriter =
            wrappedOutputFormat.getRecordWriter(
               ctx, FileOutputFormat.getTaskOutputPath(job, name));

      return new RecordWriter<Writable,SAMRecordWritable>() {
         @Override public void write(Writable ignored, SAMRecordWritable rec)
            throws IOException
         {
            try {
               wrappedRecordWriter.write(ignored, rec);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }
         @Override public void close(Reporter reporter) throws IOException {
            try {
               wrappedRecordWriter.close(ctx);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }
      };
   }
}
