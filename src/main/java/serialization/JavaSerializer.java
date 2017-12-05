package serialization;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class JavaSerializer implements IObjectSerializer {

    @Override
    public boolean isSerialObject(Object serialObject) {
        return false;
    }

    @Override
    public byte[] serialize(Object object) {
        try {
            return Serialize.serializeObj((Serializable) object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T unserialize(byte[] rawdata, Class<T> type) {
        try {
            return Serialize.unserializeObj(rawdata);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
	@Override
    public <T> T unserialize(Object serialObject, Class<?> type) {
        return (T) serialObject;
    }

    @Override
    public <T> List<T> unserializeToList(byte[] rawdata,
            Class<?>... itemTypes) {
        try {
            return Serialize.unserializeObj(rawdata);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> void unserializeToList(byte[] rawdata, List<T> list,
            Class<?>... itemTypes) {
        try {
            List<T> unserial = Serialize.unserializeObj(rawdata);
            list.addAll(unserial);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
	@Override
    public <T> List<T> unserializeToList(Object serialObject,
            Class<?>... itemTypes) {
        return (List<T>) serialObject;
    }

    @Override
    public <U, T> Map<U, T> unserializeToMap(byte[] rawdata, Class<U> keyType,
            Class<?> valueType) {
        try {
            return Serialize.unserializeObj(rawdata);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <U, T> Map<U, T> unserializeToMap(byte[] rawdata, Class<U> keyType,
            Map<U, Class<?>> valueTypes) {
        try {
            return Serialize.unserializeObj(rawdata);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <U, T> void unserializeToMap(byte[] rawdata, Map<U, T> map,
            Class<U> keyType, Class<?> valueType) {
        try {
            Map<U, T> unserial = Serialize.unserializeObj(rawdata);
            map.putAll(unserial);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <U, T> void unserializeToMap(byte[] rawdata, Map<U, T> map,
            Class<U> keyType, Map<U, Class<?>> valueTypes) {
        try {
            Map<U, T> unserial = Serialize.unserializeObj(rawdata);
            map.putAll(unserial);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
	@Override
    public <U, T> Map<U, T> unserializeToMap(Object serialObject,
            Class<U> keyType, Class<?> valueType) {
        return (Map<U, T>) serialObject;
    }

    @SuppressWarnings("unchecked")
	@Override
    public <U, T> Map<U, T> unserializeToMap(Object serialObject,
            Class<U> keyType, Map<U, Class<?>> valueTypes) {
        return (Map<U, T>) serialObject;
    }

}
