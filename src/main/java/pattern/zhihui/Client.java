package pattern.zhihui;

import java.sql.SQLException;

public class Client {

    /**
     * @param args
     */
    // No throws clause here
    public static void main(String[] args) {
//        doThrow(new SQLException());
        Integer i = new Integer(1);
        Integer j = new Integer(1);
        System.out.println(i == j);
        System.out.println(i.equals(j));
    }

    static void doThrow(Exception e) {
        Client.<RuntimeException> doThrow0(e);
    }

    @SuppressWarnings("unchecked")
    static <E extends Exception>
            void doThrow0(Exception e) throws E {
        throw (E) e;
    }

}
