// File created: 2013-02-05 15:38:08

import org.apache.hadoop.mapred.InputSplit;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

import fi.tkk.ics.hadoop.bam.FileVirtualSplit;

// Wraps FileVirtualSplit, providing the deprecated mapred API.
public class DeprecatedFileVirtualSplit implements InputSplit {
   public final FileVirtualSplit vs;

   public DeprecatedFileVirtualSplit(FileVirtualSplit v) { vs = v; }

   @Override public long getLength() { return vs.getLength(); }
   @Override public String[] getLocations() { return vs.getLocations(); }
   @Override public void write(DataOutput out) throws IOException {
      vs.write(out);
   }
   @Override public void readFields(DataInput in) throws IOException {
      vs.readFields(in);
   }
}
