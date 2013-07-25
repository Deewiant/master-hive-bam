// File created: 2013-02-18 11:16:29

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMRecord;

import fi.tkk.ics.hadoop.bam.SAMRecordWritable;

public class SAMBaseSerDe implements SerDe {
   @Override public Class<? extends Writable> getSerializedClass() {
      return SAMBaseRecord.class;
   }

   @Override public void initialize(Configuration conf, Properties tbl)
      throws SerDeException
   {}

   @Override public Object deserialize(Writable blob) throws SerDeException {
      try {
         return (SAMBaseRecord)blob;
      } catch (ClassCastException e) {
         throw new SerDeException(
            "Expected SAMBaseRecord, not " + blob.getClass().getName(), e);
      }
   }

   @Override public ObjectInspector getObjectInspector()
      throws SerDeException
   {
      return SAMBaseRecordInspector.instance;
   }

   @Override public Writable serialize(Object obj, ObjectInspector inspector)
      throws SerDeException
   {
      try {
         return (SAMBaseRecord)obj;
      } catch (ClassCastException e) {
         throw new SerDeException(
            "Expected SAMBaseRecord, not " + obj.getClass().getName(), e);
      }
   }

   @Override public SerDeStats getSerDeStats() {
      throw new RuntimeException("getSerDeStats not implemented");
   }
}

class SAMBaseRecordInspector extends StructObjectInspector {
   static SAMBaseRecordInspector instance = new SAMBaseRecordInspector();

   private static List<StructField>        fields;
   private static Map<String, StructField> fieldMap;
   static {
      fields = new ArrayList<StructField>();
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.QNAME));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.FLAG));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.RNAME));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.POS));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.MAPQ));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.CIGAR));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.RNEXT));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.PNEXT));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.TLEN));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.SEQ));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.QUAL));

      fieldMap = new HashMap<String, StructField>(fields.size(), 1);
      for (StructField f : fields)
         fieldMap.put(f.getFieldName(), f);
   }

   @Override public List<? extends StructField> getAllStructFieldRefs() {
      return fields;
   }

   @Override public StructField getStructFieldRef(String fieldName) {
      return fieldMap.get(fieldName);
   }

   @Override public Object getStructFieldData(Object data, StructField field) {
      if (data == null)
         return null;
      final SAMBaseRecord rec = (SAMBaseRecord)data;
      final SAMRecord     par = rec.getParent().get();

      switch (((SAMBaseRecordField)field).getType()) {
      case QNAME: return par.getReadName();
      case FLAG:  return (short)par.getFlags();
      case RNAME: return par.getReferenceName();
      case POS:   return rec.getPos();
      case MAPQ:  return (byte)par.getMappingQuality();
      case CIGAR: return rec.getCigarChar();
      case RNEXT: return par.getMateReferenceName();
      case PNEXT: return par.getMateAlignmentStart();
      case TLEN:  return par.getInferredInsertSize();
      case SEQ:   return rec.getBase();
      case QUAL:  return rec.getQuality();
      }
      throw new RuntimeException("Unknown field " +field);
   }

   @Override public List<Object> getStructFieldsDataAsList(Object data) {
      if (data == null)
         return null;
      final SAMBaseRecord rec = (SAMBaseRecord)data;
      final SAMRecord     par = rec.getParent().get();

      final List<Object> list = new ArrayList<Object>(fields.size());
      list.add(par.getReadName());
      list.add((short)par.getFlags());
      list.add(par.getReferenceName());
      list.add(rec.getPos());
      list.add((byte)par.getMappingQuality());
      list.add(rec.getCigarChar());
      list.add(par.getMateReferenceName());
      list.add(par.getMateAlignmentStart());
      list.add(par.getInferredInsertSize());
      list.add(rec.getBase());
      list.add(rec.getQuality());
      return list;
   }

   @Override public String   getTypeName() { return "sambaserecord"; }
   @Override public Category getCategory() { return Category.STRUCT; }
}

class SAMBaseRecordField implements StructField {
   public static enum Type {
      QNAME, FLAG, RNAME, POS, MAPQ, CIGAR, RNEXT, PNEXT, TLEN, SEQ, QUAL;
      public String getName() {
         switch (this) {
         case QNAME: return "qname";
         case FLAG:  return "flag";
         case RNAME: return "rname";
         case POS:   return "pos";
         case MAPQ:  return "mapq";
         case CIGAR: return "cigar";
         case RNEXT: return "rnext";
         case PNEXT: return "pnext";
         case TLEN:  return "tlen";
         case SEQ:   return "seq";
         case QUAL:  return "qual";
         }
         assert (false);
         throw new RuntimeException("Internal error");
      }
   }

   private final Type type;

   public SAMBaseRecordField(Type t) { type = t; }

   public Type getType() { return type; }

   @Override public String getFieldName()    { return type.getName(); }
   @Override public String getFieldComment() { return null; }

   @Override public ObjectInspector getFieldObjectInspector() {
      switch (type) {
         case QNAME: case RNAME: case RNEXT:
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;

         case FLAG:
            return PrimitiveObjectInspectorFactory.javaShortObjectInspector;

         case POS: case PNEXT: case TLEN:
            return PrimitiveObjectInspectorFactory.javaIntObjectInspector;

         case MAPQ: case CIGAR: case SEQ: case QUAL:
            return PrimitiveObjectInspectorFactory.javaByteObjectInspector;
      }
      assert (false);
      throw new RuntimeException("Internal error");
   }
}
