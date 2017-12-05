package serialization;


public class SerializeServiceFactory {

    private static IObjectSerializer objectSerializer = new JavaSerializer();

    public static IObjectSerializer getObjectSerializer() {
        return objectSerializer;
    }
}
