// File created: 2013-02-15 10:29:29

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import net.sf.samtools.Cigar;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMRecord;

import fi.tkk.ics.hadoop.bam.SAMRecordWritable;

public class SAMBaseRecord implements Writable {
   private SAMRecordWritable parent;

   private int           idx, cigarElemIdx, cigarPos;
   private CigarOperator cigarOp;
   private byte          base, quality;
   private boolean       noSeq, noQuals, noCigar, ownParent;

   public SAMRecordWritable getParent() { return parent; }

   public int getPos() { return parent.get().getAlignmentStart() + idx; }

   public CigarOperator getCigarOp() { return cigarOp; }
   public byte          getQuality() { return quality; }
   public byte          getBase   () { return base; }

   public byte getCigarChar() {
      return cigarOp == null ? (byte)'*'
                             : CigarOperator.enumToCharacter(cigarOp);
   }

   public void setParent(SAMRecordWritable wrec) {
      parent = wrec;
      ownParent = false;

      final SAMRecord rec = parent.get();

      idx = 0;

      noCigar = rec.getCigar().isEmpty();
      if (noCigar)
         cigarOp = null;
      else {
         cigarElemIdx = 0;
         cigarPos     = 0;
         cigarOp = rec.getCigar().getCigarElement(0).getOperator();
      }

      gotoNextAcceptableCigarOp();
      initBaseAndQuality();
   }

   private void initBaseAndQuality() {
      final SAMRecord rec = parent.get();

      final byte[] bases = rec.getReadBases();
      noSeq = bases == SAMRecord.NULL_SEQUENCE;
      base = noSeq ? -1 : bases[idx];

      final byte[] quals = rec.getBaseQualities();
      noQuals = quals == SAMRecord.NULL_QUALS;
      quality = noQuals ? -1 : quals[idx];
   }

   public boolean gotoNextBase() {
      final SAMRecord rec = parent.get();

      if (++idx == rec.getReadLength())
         return false;

      if (!noSeq)   base    = rec.getReadBases    ()[idx];
      if (!noQuals) quality = rec.getBaseQualities()[idx];

      if (!noCigar) {
         final Cigar        cigar     = rec.getCigar();
         final CigarElement cigarElem = cigar.getCigarElement(cigarElemIdx);

         ++cigarPos;
         if (cigarPos == cigarElem.getLength()) {
            ++cigarElemIdx;
            cigarOp = cigar.getCigarElement(cigarElemIdx).getOperator();
            cigarPos = 0;
            gotoNextAcceptableCigarOp();
         }
      }
      return true;
   }

   private void gotoNextAcceptableCigarOp() {
      assert !noCigar;

      // This shouldn't run off the end of the array since idx hasn't reached
      // getReadLength() yet.
      final Cigar cigar = parent.get().getCigar();
      while (!cigarOp.consumesReadBases())
         cigarOp = cigar.getCigarElement(++cigarElemIdx).getOperator();
   }

   @Override public void write(DataOutput out) throws IOException {
      parent.write(out);
      out.writeInt(idx);
      if (!noCigar) {
         out.writeInt(cigarElemIdx);
         out.writeInt(cigarPos);
      }
   }
   @Override public void readFields(DataInput in) throws IOException {
      if (parent == null || !ownParent) {
         parent = new SAMRecordWritable();
         ownParent = true;
      }
      parent.readFields(in);
      idx = in.readInt();

      final SAMRecord rec = parent.get();

      noCigar = rec.getCigar().isEmpty();
      if (noCigar)
         cigarOp = null;
      else {
         cigarElemIdx = in.readInt();
         cigarPos     = in.readInt();
         cigarOp = rec.getCigar().getCigarElement(cigarElemIdx).getOperator();
      }
      
      initBaseAndQuality();
   }
}
