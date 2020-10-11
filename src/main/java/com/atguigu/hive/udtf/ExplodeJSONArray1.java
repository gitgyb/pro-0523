package com.atguigu.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName ExplodeJSONArray1
 * @Desciption TODO
 * @Author 86186
 * @Date 2020/10/5 12:04
 * @Version 1.0
 **/
public class ExplodeJSONArray1 extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //1.参数合法性检测
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentException("ExplodeJSON只需要一个参数");
        }
        //2.第一个参数必须为string
        if (!"string".equals(argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName())) {
            throw new UDFArgumentException("json_array_to_struct_array的第一个参数应为string类型");
        }
        //3.指定返回值默认列名
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("action");
        //4.执行返回值类型
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //5.最终的返回
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    public void process(Object[] objects) throws HiveException {
        //1.获取传入的参数
        String jsonArray = objects[0].toString();
        //2.将string转换为json数组
        JSONArray actions = new JSONArray(jsonArray);
        //3.循环一次,取出数组中的json，并写出
        for (int i = 0; i < actions.length() ; i++) {
            String[] result = new String[1];
             result[0] = actions.getString(i);
             forward(result);

        }

    }

    public void close() throws HiveException {

    }
}
