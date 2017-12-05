package serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Serialize {

    public static byte[] serializeObj(Serializable obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(baos);
        os.writeObject(obj);
        return baos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T unserializeObj(
            byte[] bobj)
            throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bobj);
        ObjectInputStream os = new ObjectInputStream(bais);
        return (T) os.readObject();
    }

}
