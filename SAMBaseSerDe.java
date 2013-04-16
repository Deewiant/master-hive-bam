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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMTagUtil;

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

      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.OPTS_CHAR));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.OPTS_INT));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.OPTS_FLOAT));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.OPTS_STRING));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.OPTS_ARR_INT8));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.OPTS_ARR_INT16));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.OPTS_ARR_INT32));
      fields.add(new SAMBaseRecordField(SAMBaseRecordField.Type.OPTS_ARR_FLOAT));

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

      final SAMBaseRecordField.Type ty = ((SAMBaseRecordField)field).getType();

      switch (ty) {
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
      for (final SAMRecord.SAMTagAndValue tav : par.getAttributes())
         if (tav.value.getClass() == givenClass)
            optsMap.put(u.makeBinaryTag(tav.tag), storedClass.cast(tav.value));
      return optsMap;
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
      for (final SAMRecord.SAMTagAndValue tav : par.getAttributes()) {
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

   @Override public String   getTypeName() { return "sambaserecord"; }
   @Override public Category getCategory() { return Category.STRUCT; }
}

class SAMBaseRecordField implements StructField {
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
