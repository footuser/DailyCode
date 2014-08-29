package util.zhihui;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * list序列化
 * 
 * @author zhihuiqiu
 * @param <M>
 */
public class ListTranscoder<M extends Serializable> extends SerializeTranscoder {

    @SuppressWarnings("unchecked")
    public List<M> deserialize(byte[] in) {
        List<M> list = new ArrayList<M>();
        ByteArrayInputStream bis = null;
        ObjectInputStream is = null;
        try {
            if (in != null) {
                bis = new ByteArrayInputStream(in);
                is = new ObjectInputStream(bis);
                while (bis.available() > 0) { // while(true) will throw
                                              // EOFException
                    M m = (M) is.readObject();
                    if (m == null) {
                        break;
                    }

                    list.add(m);

                }
                is.close();
                bis.close();
            }
        } catch (IOException e) {
            logger.error("Caught IOException decoding %d bytes of data" + in == null ? 0 : in.length, e);
        } catch (ClassNotFoundException e) {
            logger.error("Caught CNFE decoding %d bytes of data" + in == null ? 0 : in.length, e);
        } finally {
            close(is);
            close(bis);
        }

        return list;
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] serialize(Object value) {
        if (value == null)
            throw new NullPointerException("Can't serialize null");

        List<M> values = (List<M>) value;

        byte[] results = null;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream os = null;

        try {
            bos = new ByteArrayOutputStream();
            os = new ObjectOutputStream(bos);
            for (M m : values) {
                os.writeObject(m);
            }

            os.close();
            bos.close();
            results = bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalArgumentException("Non-serializable object", e);
        } finally {
            close(os);
            close(bos);
        }

        return results;
    }

}