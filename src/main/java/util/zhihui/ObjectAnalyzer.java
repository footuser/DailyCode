package util.zhihui;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;

public class ObjectAnalyzer {
    public static String toString(Object obj)
    {
        @SuppressWarnings("rawtypes")
        Class cl = obj.getClass();
        String r = cl.getName();
        // inspect the fields of this class and all superclasses
        do
        {
            r += "[";
            Field[] fields = cl.getDeclaredFields();
            AccessibleObject.setAccessible(fields, true);
            // get the names and values of all fields
            for (int i = 0; i < fields.length; i++)
            {
                Field f = fields[i];
                r += f.getName() + "=";
                try
                {
                    Object val = f.get(obj);
                    r += val.toString();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (i < fields.length - 1)
                    r += ",";
            }
            r += "]";
            cl = cl.getSuperclass();
        } while (cl != Object.class);
        return r;
    }
    
    public static void main(String[] args) {
        System.out.println(ObjectAnalyzer.toString(new ObjectTranscoder<String>() ));
    }
}
