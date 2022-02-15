package io.box;

public interface Database {

    Database set(final String key, final String value);
    String get(final String key);
    Database delete(final String key);
    long count(final String value);
    Database begin();
    Database rollback();
    Database commit();


}
