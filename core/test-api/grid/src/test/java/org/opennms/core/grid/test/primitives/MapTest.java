package org.opennms.core.grid.test.primitives;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 * Other contributors include Andrew Wright, Jeffrey Hayes, 
 * Pat Fisher, Mike Judd.
 * 
 * Adapted by Jesse White to work with the primitives provided
 * via the grid provider interface.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MapTest extends JSR166TestCase{
    private static Map getNewMap() {
        return gridProvider.getMap("map" + ROLLING_ID++);
    }

    /**
     * Create a map from Integers 1-5 to Strings "A"-"E".
     */
    private static Map map5() {   
        Map map = getNewMap();
        assertTrue(map.isEmpty());
        map.put(one, "A");
        map.put(two, "B");
        map.put(three, "C");
        map.put(four, "D");
        map.put(five, "E");
        assertFalse(map.isEmpty());
        assertEquals(5, map.size());
        return map;
    }

    /**
     *  clear removes all pairs
     */
    @Test
    public void testClear() {
        Map map = map5();
        map.clear();
        assertEquals(map.size(), 0);
    }

    /**
     *  Maps with same contents are equal
     */
    @Test
    public void testEquals() {
        Map map1 = map5();
        Map map2 = map5();
        assertEquals(map1, map2);
        assertEquals(map2, map1);
        map1.clear();
        assertFalse(map1.equals(map2));
        assertFalse(map2.equals(map1));
    }

    /**
     *  contains returns true for contained value
     */
    @Test
    public void testContains() {
        Map map = map5();
        assertTrue(map.containsValue("A"));
        assertFalse(map.containsValue("Z"));
    }
    
    /**
     *  containsKey returns true for contained key
     */
    @Test
    public void testContainsKey() {
        Map map = map5();
        assertTrue(map.containsKey(one));
        assertFalse(map.containsKey(zero));
    }

    /**
     *  containsValue returns true for held values
     */
    @Test
    public void testContainsValue() {
        Map map = map5();
        assertTrue(map.containsValue("A"));
        assertFalse(map.containsValue("Z"));
    }

    /**
     *   enumeration returns an enumeration containing the correct
     *   elements
     */
    @Test
    public void testEnumeration() {
        Map map = map5();
        Iterator it = map.values().iterator();
        int count = 0;
        while(it.hasNext()){
            count++;
            it.next();
        }
        assertEquals(5, count);
    }

    /**
     *  get returns the correct element at the given key,
     *  or null if not present
     */
    @Test
    public void testGet() {
        Map map = map5();
        assertEquals("A", (String)map.get(one));
        Map empty = getNewMap();
        assertNull(empty.get("anything"));
    }

    /**
     *  isEmpty is true of empty map and false for non-empty
     */
    @Test
    public void testIsEmpty() {
        Map empty = getNewMap();
        Map map = map5();
        assertTrue(empty.isEmpty());
        assertFalse(map.isEmpty());
    }

    /**
     *   keys returns an enumeration containing all the keys from the map
     */
    @Test
    public void testKeys() {
        Map map = map5();
        Iterator it = map.keySet().iterator();
        int count = 0;
        while(it.hasNext()){
            count++;
            it.next();
        }
        assertEquals(5, count);
    }

    /**
     *   keySet returns a Set containing all the keys
     */
    @Test
    public void testKeySet() {
        Map map = map5();
        Set s = map.keySet();
        assertEquals(5, s.size());
        assertTrue(s.contains(one));
        assertTrue(s.contains(two));
        assertTrue(s.contains(three));
        assertTrue(s.contains(four));
        assertTrue(s.contains(five));
    }

    /**
     * values collection contains all values
     */
    @Test
    public void testValues() {
        Map map = map5();
        Collection s = map.values();
        assertEquals(5, s.size());
        assertTrue(s.contains("A"));
        assertTrue(s.contains("B"));
        assertTrue(s.contains("C"));
        assertTrue(s.contains("D"));
        assertTrue(s.contains("E"));
    }

    /**
     * entrySet contains all pairs
     */
    @Test
    public void testEntrySet() {
        Map map = map5();
        Set s = map.entrySet();
        assertEquals(5, s.size());
        Iterator it = s.iterator();
        while (it.hasNext()) {
            Map.Entry e = (Map.Entry) it.next();
            assertTrue( 
                       (e.getKey().equals(one) && e.getValue().equals("A")) ||
                       (e.getKey().equals(two) && e.getValue().equals("B")) ||
                       (e.getKey().equals(three) && e.getValue().equals("C")) ||
                       (e.getKey().equals(four) && e.getValue().equals("D")) ||
                       (e.getKey().equals(five) && e.getValue().equals("E")));
        }
    }

    /**
     *   putAll  adds all key-value pairs from the given map
     */
    @Test
    public void testPutAll() {
        Map empty = getNewMap();
        Map map = map5();
        empty.putAll(map);
        assertEquals(5, empty.size());
        assertTrue(empty.containsKey(one));
        assertTrue(empty.containsKey(two));
        assertTrue(empty.containsKey(three));
        assertTrue(empty.containsKey(four));
        assertTrue(empty.containsKey(five));
    }

    /**
     *   remove removes the correct key-value pair from the map
     */
    @Test
    public void testRemove() {
        Map map = map5();
        map.remove(five);
        assertEquals(4, map.size());
        assertFalse(map.containsKey(five));
    }
    /**
     *   size returns the correct values
     */
    @Test
    public void testSize() {
        Map map = map5();
        Map empty = getNewMap();
        assertEquals(0, empty.size());
        assertEquals(5, map.size());
    }

    /**
     * toString contains toString of elements
     */
    @Test
    public void testToString() {
        Map map = map5();
        String s = map.toString();
        for (int i = 1; i <= 5; ++i) {
            assertTrue(s.indexOf(String.valueOf(i)) >= 0);
        }
    }

    /**
     * SetValue of an EntrySet entry sets value in the map.
     */
    @Test
    public void testSetValueWriteThrough() {
        // Adapted from a bug report by Eric Zoerner 
        Map map = getNewMap();
        assertTrue(map.isEmpty());
        for (int i = 0; i < 20; i++)
            map.put(new Integer(i), new Integer(i));
        assertFalse(map.isEmpty());
        Map.Entry entry1 = (Map.Entry)map.entrySet().iterator().next();
        
        // assert that entry1 is not 16
        assertTrue("entry is 16, test not valid",
                   !entry1.getKey().equals(new Integer(16)));
        
        // remove 16 (a different key) from map 
        // which just happens to cause entry1 to be cloned in map
        map.remove(new Integer(16));
        entry1.setValue("XYZ");
        assertTrue(map.containsValue("XYZ")); // fails
    }
}
