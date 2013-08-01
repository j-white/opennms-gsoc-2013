package org.opennms.core.grid.test.primitives;

import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

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
public class SetTest extends JSR166TestCase {

    private static Set getNewSet() {
        return gridProvider.getSet("set" + ROLLING_ID++);
    }

    static Set populatedSet(int n) {
        Set a = getNewSet();
        assertTrue(a.isEmpty());
        for (int i = 0; i < n; ++i) 
            a.add(new Integer(i));
        assertFalse(a.isEmpty());
        assertEquals(n, a.size());
        return a;
    }

    /**
     * Default-constructed set is empty
     */
    @Test
    public void testConstructor() {
        Set a = getNewSet();
        assertTrue(a.isEmpty());
    }

    /**
     *   addAll  adds each element from the given collection
     */
    @Test
    public void testAddAll() {
        Set full = populatedSet(3);
        Vector v = new Vector();
        v.add(three);
        v.add(four);
        v.add(five);
        full.addAll(v);
        assertEquals(6, full.size());
    }

    /**
     *   addAll adds each element from the given collection that did not
     *  already exist in the set
     */
    @Test
    public void testAddAll2() {
        Set full = populatedSet(3);
        Vector v = new Vector();
        v.add(three);
        v.add(four);
        v.add(one); // will not add this element
        full.addAll(v);
        assertEquals(5, full.size());
    }

    /**
     *   add will not add the element if it already exists in the set
     */
    @Test
    public void testAdd2() {
        Set full = populatedSet(3);
        full.add(one);
        assertEquals(3, full.size());
    }

    /**
     *   add  adds the element when it does not exist
     *   in the set
     */
    @Test
    public void testAdd3() {
        Set full = populatedSet(3);
        full.add(three);
        assertTrue(full.contains(three));
    }

    /**
     *   clear  removes all elements from the set
     */
    @Test
    public void testClear() {
        Set full = populatedSet(3);
        full.clear();
        assertEquals(0, full.size());
    }

    /**
     *   contains returns true for added elements
     */
    @Test
    public void testContains() {
        Set full = populatedSet(3);
        assertTrue(full.contains(one));
        assertFalse(full.contains(five));
    }
    
    /**
     *   containsAll returns true for collections with subset of elements
     */
    @Test
    public void testContainsAll() {
        Set full = populatedSet(3);
        Vector v = new Vector();
        v.add(one);
        v.add(two);
        assertTrue(full.containsAll(v));
        v.add(six);
        assertFalse(full.containsAll(v));
    }

    /**
     *   isEmpty is true when empty, else false
     */
    @Test
    public void testIsEmpty() {
        Set empty = getNewSet();
        Set full = populatedSet(3);
        assertTrue(empty.isEmpty());
        assertFalse(full.isEmpty());
    }

    /**
     *   iterator() returns an iterator containing the elements of the set 
     */
    @Test
    public void testIterator() {
        Set full = populatedSet(3);
        Iterator i = full.iterator();
        int j;
        for(j = 0; i.hasNext(); j++) {
            ((Integer)i.next()).intValue();
        }
        assertEquals(3, j);
    }

    /**
     *   removeAll  removes all elements from the given collection
     */
    @Test
    public void testRemoveAll() {
        Set full = populatedSet(3);
        Vector v = new Vector();
        v.add(one);
        v.add(two);
        full.removeAll(v);
        assertEquals(1, full.size());
    }

    /**
     * remove removes an element
     */
    @Test
    public void testRemove() {
        Set full = populatedSet(3);
        full.remove(one);
        assertFalse(full.contains(one));
        assertEquals(2, full.size());
    }

    /**
     *   size returns the number of elements
     */
    @Test
    public void testSize() {
        Set empty = getNewSet();
        Set full = populatedSet(3);
        assertEquals(3, full.size());
        assertEquals(0, empty.size());
    }

    /**
     *  toArray throws an ArrayStoreException when the given array can
     *  not store the objects inside the set
     */
    @Test
    public void testToArray_ArrayStoreException() {
        try {
            Set c = getNewSet();
            c.add("zfasdfsdf");
            c.add("asdadasd");
            c.toArray(new Long[5]);
            shouldThrow();
        } catch(ArrayStoreException e){}
    }
}
