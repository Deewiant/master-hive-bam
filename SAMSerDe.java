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
// File created: 2013-02-05 13:37:56

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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMTagUtil;

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
      fields.add(new SAMRecordField(SAMRecordField.Type.QNAME));
      fields.add(new SAMRecordField(SAMRecordField.Type.FLAG));
      fields.add(new SAMRecordField(SAMRecordField.Type.RNAME));
      fields.add(new SAMRecordField(SAMRecordField.Type.POS));
      fields.add(new SAMRecordField(SAMRecordField.Type.MAPQ));
      fields.add(new SAMRecordField(SAMRecordField.Type.CIGAR));
      fields.add(new SAMRecordField(SAMRecordField.Type.RNEXT));
      fields.add(new SAMRecordField(SAMRecordField.Type.PNEXT));
      fields.add(new SAMRecordField(SAMRecordField.Type.TLEN));
      fields.add(new SAMRecordField(SAMRecordField.Type.SEQ));
      fields.add(new SAMRecordField(SAMRecordField.Type.QUAL));

      fields.add(new SAMRecordField(SAMRecordField.Type.OPTS_CHAR));
      fields.add(new SAMRecordField(SAMRecordField.Type.OPTS_INT));
      fields.add(new SAMRecordField(SAMRecordField.Type.OPTS_FLOAT));
      fields.add(new SAMRecordField(SAMRecordField.Type.OPTS_STRING));
      fields.add(new SAMRecordField(SAMRecordField.Type.OPTS_ARR_INT8));
      fields.add(new SAMRecordField(SAMRecordField.Type.OPTS_ARR_INT16));
      fields.add(new SAMRecordField(SAMRecordField.Type.OPTS_ARR_INT32));
      fields.add(new SAMRecordField(SAMRecordField.Type.OPTS_ARR_FLOAT));

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

      final SAMRecordField.Type ty = ((SAMRecordField)field).getType();

      switch (ty) {
      case QNAME: return rec.getReadName();
      case FLAG:  return (short)rec.getFlags();
      case RNAME: return rec.getReferenceName();
      case POS:   return rec.getAlignmentStart();
      case MAPQ:  return (byte)rec.getMappingQuality();
      case CIGAR: return rec.getCigarString();
      case RNEXT: return rec.getMateReferenceName();
      case PNEXT: return rec.getMateAlignmentStart();
      case TLEN:  return rec.getInferredInsertSize();
      case SEQ:   return rec.getReadString();
      case QUAL:  return rec.getBaseQualityString();
      }

      final Map<Short,Object> optsMap = new HashMap<Short,Object>();

      final Class<?> givenClass, storedClass;
      switch (ty) {
      case OPTS_CHAR: givenClass = char.class; storedClass = byte.class; break;

      case OPTS_INT:    givenClass = storedClass = int.class; break;
      case OPTS_FLOAT:  givenClass = storedClass = float.class; break;
      case OPTS_STRING: givenClass = storedClass = String.class; break;

      case OPTS_ARR_INT8:  givenClass = storedClass = byte [].class; break;
      case OPTS_ARR_INT16: givenClass = storedClass = short[].class; break;
      case OPTS_ARR_INT32: givenClass = storedClass = int  [].class; break;
      case OPTS_ARR_FLOAT: givenClass = storedClass = float[].class; break;

      default: throw new RuntimeException("Unknown field " +field);
      }

      final SAMTagUtil u = SAMTagUtil.getSingleton();
      for (final SAMRecord.SAMTagAndValue tav : rec.getAttributes())
         if (tav.value.getClass() == givenClass)
            optsMap.put(u.makeBinaryTag(tav.tag), storedClass.cast(tav.value));
      return optsMap;
   }

   @Override public List<Object> getStructFieldsDataAsList(Object data) {
      if (data == null)
         return null;
      final SAMRecord rec = (SAMRecord)data;

      final List<Object> list = new ArrayList<Object>(fields.size());
      list.add(rec.getReadName());
      list.add((short)rec.getFlags());
      list.add(rec.getReferenceName());
      list.add(rec.getAlignmentStart());
      list.add((byte)rec.getMappingQuality());
      list.add(rec.getCigarString());
      list.add(rec.getMateReferenceName());
      list.add(rec.getMateAlignmentStart());
      list.add(rec.getInferredInsertSize());
      list.add(rec.getReadString());
      list.add(rec.getBaseQualityString());

      final Map<Short,Byte>    optsChar   = new HashMap<Short,Byte>();
      final Map<Short,Integer> optsInt    = new HashMap<Short,Integer>();
      final Map<Short,Float>   optsFloat  = new HashMap<Short,Float>();
      final Map<Short,String>  optsString = new HashMap<Short,String>();
      final Map<Short,byte[]>  optsAInt8  = new HashMap<Short,byte[]>();
      final Map<Short,short[]> optsAInt16 = new HashMap<Short,short[]>();
      final Map<Short,int[]>   optsAInt32 = new HashMap<Short,int[]>();
      final Map<Short,float[]> optsAFloat = new HashMap<Short,float[]>();

      list.add(optsChar);
      list.add(optsInt);
      list.add(optsFloat);
      list.add(optsString);
      list.add(optsAInt8);
      list.add(optsAInt16);
      list.add(optsAInt32);
      list.add(optsAFloat);

      final SAMTagUtil u = SAMTagUtil.getSingleton();
      for (final SAMRecord.SAMTagAndValue tav : rec.getAttributes()) {
         final short key = u.makeBinaryTag(tav.tag);
         final Class<?> c = tav.value.getClass();

         if (c == char.class)
            optsChar.put(key, (Byte)tav.value);
         else if (c == int.class)
            optsInt.put(key, (Integer)tav.value);
         else if (c == float.class)
            optsFloat.put(key, (Float)tav.value);
         else if (c == String.class)
            optsString.put(key, (String)tav.value);
         else if (c == byte[].class)
            optsAInt8.put(key, (byte[])tav.value);
         else if (c == short[].class)
            optsAInt16.put(key, (short[])tav.value);
         else if (c == int[].class)
            optsAInt32.put(key, (int[])tav.value);
         else if (c == float[].class)
            optsAFloat.put(key, (float[])tav.value);
         else
            throw new RuntimeException("Unknown value type for tag: "+c);
      }
      return list;
   }

   @Override public String   getTypeName() { return "samrecord"; }
   @Override public Category getCategory() { return Category.STRUCT; }
}

class SAMRecordField implements StructField {
   // We have to split the optional fields by type because Hive doesn't have
   // dynamically typed columns. There's no way of making an ObjectInspector
   // that can give a value-dependent type.
   public static enum Type {
      QNAME, FLAG, RNAME, POS, MAPQ, CIGAR, RNEXT, PNEXT, TLEN, SEQ, QUAL,
      OPTS_CHAR, OPTS_INT, OPTS_FLOAT, OPTS_STRING,
      OPTS_ARR_INT8, OPTS_ARR_INT16, OPTS_ARR_INT32, OPTS_ARR_FLOAT;
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

         case OPTS_CHAR:      return "opts_char";
         case OPTS_INT:       return "opts_int";
         case OPTS_FLOAT:     return "opts_float";
         case OPTS_STRING:    return "opts_string";
         case OPTS_ARR_INT8:  return "opts_arr_int8";
         case OPTS_ARR_INT16: return "opts_arr_int16";
         case OPTS_ARR_INT32: return "opts_arr_int32";
         case OPTS_ARR_FLOAT: return "opts_arr_float";
         }
         assert (false);
         throw new RuntimeException("Internal error");
      }
   }

   private final SAMRecordField.Type type;

   public SAMRecordField(SAMRecordField.Type t) { type = t; }

   public Type getType() { return type; }

   @Override public String getFieldName()    { return type.getName(); }
   @Override public String getFieldComment() { return null; }

   @Override public ObjectInspector getFieldObjectInspector() {
      switch (type) {
         case QNAME: case RNAME: case CIGAR: case RNEXT: case SEQ: case QUAL:
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;

         case FLAG:
            return PrimitiveObjectInspectorFactory.javaShortObjectInspector;

         case MAPQ:
            return PrimitiveObjectInspectorFactory.javaByteObjectInspector;

         case POS: case PNEXT: case TLEN:
            return PrimitiveObjectInspectorFactory.javaIntObjectInspector;

         case OPTS_CHAR:
            return ObjectInspectorFactory.getStandardMapObjectInspector(
               PrimitiveObjectInspectorFactory.javaShortObjectInspector,
               PrimitiveObjectInspectorFactory.javaByteObjectInspector);
         case OPTS_INT:
            return ObjectInspectorFactory.getStandardMapObjectInspector(
               PrimitiveObjectInspectorFactory.javaShortObjectInspector,
               PrimitiveObjectInspectorFactory.javaIntObjectInspector);
         case OPTS_FLOAT:
            return ObjectInspectorFactory.getStandardMapObjectInspector(
               PrimitiveObjectInspectorFactory.javaShortObjectInspector,
               PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
         case OPTS_STRING:
            return ObjectInspectorFactory.getStandardMapObjectInspector(
               PrimitiveObjectInspectorFactory.javaShortObjectInspector,
               PrimitiveObjectInspectorFactory.javaStringObjectInspector);
         case OPTS_ARR_INT8:
            return ObjectInspectorFactory.getStandardMapObjectInspector(
               PrimitiveObjectInspectorFactory.javaShortObjectInspector,
               ObjectInspectorFactory.getStandardListObjectInspector(
                  PrimitiveObjectInspectorFactory.javaByteObjectInspector));
         case OPTS_ARR_INT16:
            return ObjectInspectorFactory.getStandardMapObjectInspector(
               PrimitiveObjectInspectorFactory.javaShortObjectInspector,
               ObjectInspectorFactory.getStandardListObjectInspector(
                  PrimitiveObjectInspectorFactory.javaShortObjectInspector));
         case OPTS_ARR_INT32:
            return ObjectInspectorFactory.getStandardMapObjectInspector(
               PrimitiveObjectInspectorFactory.javaShortObjectInspector,
               ObjectInspectorFactory.getStandardListObjectInspector(
                  PrimitiveObjectInspectorFactory.javaIntObjectInspector));
         case OPTS_ARR_FLOAT:
            return ObjectInspectorFactory.getStandardMapObjectInspector(
               PrimitiveObjectInspectorFactory.javaShortObjectInspector,
               ObjectInspectorFactory.getStandardListObjectInspector(
                  PrimitiveObjectInspectorFactory.javaFloatObjectInspector));
      }
      assert (false);
      throw new RuntimeException("Internal error");
   }
}
