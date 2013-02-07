// File created: 2013-02-05 13:37:56

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import net.sf.samtools.SAMRecord;

import fi.tkk.ics.hadoop.bam.SAMRecordWritable;

public class SAMSerDe implements SerDe {
   private SAMRecordWritable samRecordWritable = new SAMRecordWritable();

   @Override public Class<? extends Writable> getSerializedClass() {
      return SAMRecordWritable.class;
   }

   @Override public void initialize(Configuration conf, Properties tbl)
      throws SerDeException
   {}

   @Override public Object deserialize(Writable blob) throws SerDeException {
      try {
         return ((SAMRecordWritable)blob).get();
      } catch (ClassCastException e) {
         throw new SerDeException(
            "Expected SAMRecordWritable, not " + blob.getClass().getName(), e);
      }
   }

   @Override public ObjectInspector getObjectInspector()
      throws SerDeException
   {
      return SAMRecordInspector.instance;
   }

   @Override public Writable serialize(Object obj, ObjectInspector inspector)
      throws SerDeException
   {
      try {
         samRecordWritable.set((SAMRecord)obj);
         return samRecordWritable;
      } catch (ClassCastException e) {
         throw new SerDeException(
            "Expected SAMRecord, not " + obj.getClass().getName(), e);
      }
   }

   @Override public SerDeStats getSerDeStats() {
      throw new RuntimeException("getSerDeStats not implemented");
   }
}

class SAMRecordInspector extends StructObjectInspector {
   static SAMRecordInspector instance = new SAMRecordInspector();

   private static List<StructField>        fields;
   private static Map<String, StructField> fieldMap;
   static {
      fields = new ArrayList<StructField>();
      fields.add(new QNameField());

      // TODO: remaining fields

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
      final SAMRecord rec = (SAMRecord)data;

      if (field instanceof QNameField) {
         return rec.getReadName();
      }

      throw new RuntimeException("other fields not implemented");
   }

   @Override public List<Object> getStructFieldsDataAsList(Object data) {
      if (data == null)
         return null;
      final SAMRecord rec = (SAMRecord)data;

      final List<Object> list = new ArrayList<Object>(fields.size());
      list.add(rec.getReadName());
      return list;
   }

   @Override public String   getTypeName() { return "samrecord"; }
   @Override public Category getCategory() { return Category.STRUCT; }
}

class QNameField implements StructField {
   static class Inspector
         extends AbstractPrimitiveJavaObjectInspector
         implements StringObjectInspector
   {
      public Inspector() {
         super(PrimitiveObjectInspectorUtils.stringTypeEntry);
      }

      @Override public Text getPrimitiveWritableObject(Object o) {
         return new Text((String)o);
      }
      @Override public String getPrimitiveJavaObject(Object o) {
         return (String)o;
      }
   }
   private static ObjectInspector insp = new Inspector();

   @Override public String          getFieldName()            {return "qname";}
   @Override public ObjectInspector getFieldObjectInspector() {return insp;}
   @Override public String          getFieldComment()         {return null;}
}
