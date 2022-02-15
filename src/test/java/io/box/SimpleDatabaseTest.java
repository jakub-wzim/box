package io.box;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class SimpleDatabaseTest {


    @Test
    public void setAndGet() {
        Database db = new SimpleDatabase();
        db.set("a", "10");

        assertEquals("10", db.get("a"));
    }

    @Test
    public void setAndDelete() {
        Database db = new SimpleDatabase();
        db.set("a", "10");
        db.delete("a");
        assertTrue(db.get("a") == null);
    }

    @Test
    public void setAndCount() {
        Database db = new SimpleDatabase();
        db.set("a", "10");
        db.set("b", "10");

        assertTrue(db.count("10") == 2);
        assertTrue(db.count("20") == 0);

    }

    @Test
    public void setDeleteAndCount() {
        Database db = new SimpleDatabase();
        db.set("a", "10");
        db.set("b", "10");

        db.delete("a");
        assertTrue(db.count("10") == 1);

    }

    @Test
    public void setDeleteResetAndCount() {
        Database db = new SimpleDatabase();
        db.set("a", "10");
        db.set("b", "10");

        db.delete("a");
        db.set("b", "30");
        assertTrue(true);

    }

    @Test
    public void setTx() {
        Database db = new SimpleDatabase();
        db.begin();
        db.set("a", "10");
        assertEquals("10", db.get("a"));
        assertEquals(1, db.count("10"));

        db.begin();
        db.set("a", "20");
        assertEquals("20", db.get("a"));
        assertEquals(0, db.count("10"));
        assertEquals(1, db.count("20"));

        db.rollback();
        assertEquals("10", db.get("a"));
        assertEquals(1, db.count("10"));
        assertEquals(0, db.count("20"));

        db.rollback();
        assertNull(db.get("a"));
    }

    @Test
    public void tx2() {
        Database db = new SimpleDatabase();
        db.begin();
        db.set("a", "30");
        db.begin();
        db.set("a", "40");
        db.commit();
        assertEquals("40", db.get("a"));
        db.rollback();
    }

    @Test
    public void tx3() {
        Database db = new SimpleDatabase();
        db.set("a", "50");
        db.begin();
        assertEquals("50", db.get("a"));
        db.set("a", "60");
        db.begin();
        db.delete("a");
        assertNull(db.get("a"));
        db.rollback();
        assertEquals("60", db.get("a"));
        db.commit();
        assertEquals("60", db.get("a"));

    }

    @Test
    public void tx4() {
        Database db = new SimpleDatabase();
        db.set("a", "10");
        db.begin();
        assertEquals(1, db.count("10"));
        db.begin();
        db.delete("a");
        assertEquals(0, db.count("10"));
        db.rollback();
        assertEquals(1, db.count("10"));

    }
}
