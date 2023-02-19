package org.vps.web.impl;

import java.util.*;
import java.lang.reflect.Array;


/**
 * This map implementation will preserve the same order of elements,
 * as they were added to it.
 *
 * @author Pawel S. Veselov
 */
public class SequenceMap extends AbstractMap {

    Map.Entry [] body;

    public SequenceMap() {
	body = new Map.Entry[0];
    }

    public Set entrySet() {
	return new SequenceSet();
    }

    public Object put(Object key, Object value) {

	Iterator i = entrySet().iterator();
	SequenceEntry se = null;
	while (i.hasNext()) {
	    SequenceEntry cur_se = (SequenceEntry)i.next();
	    if (cur_se.getKey().equals(key)) {
		se = cur_se;
		break;
	    }
	}

	if (se == null) {
	    se = new SequenceEntry();
	    Map.Entry [] newBody = new Map.Entry[body.length+1];
	    System.arraycopy(body, 0, newBody, 0, body.length);
	    newBody[body.length] = se;
	    body = newBody;
	}

	return ((Object[])se.setValue(new Object[] {key, value}))[1];
    }

    int findByKey(Object key) {

	for (int i=0; i<body.length; i++) {
	    if (key.equals(body[i].getKey())) {
		return i;
	    }
	}
	return -1;
    }

    public void putAll(Map m) {

	Iterator i = m.keySet().iterator();
	while (i.hasNext()) {
	    Object key = i.next();
	    Object val = m.get(key);
	    put(key, val);
	}
    }

    class SequenceEntry implements Map.Entry {

	private Object key;
	private Object value;

	public boolean equals(Object o) {

	    if ((o== null) || (!(o instanceof SequenceEntry))) {
		return false;
	    }

	    SequenceEntry rhs = (SequenceEntry)o;

	    return key.equals(rhs.key) && value.equals(rhs.value);
	}

	public Object getKey() {
	    return key;
	}
	public Object getValue() {
	    return value;
	}
	public int hashCode() {
	    return key.hashCode();
	}

	public Object setValue(Object newVal) {
	    Object [] realValue = (Object[])newVal;

	    Object [] cur = new Object[2];
	    cur[0] = key;
	    cur[1] = value;

	    key = realValue[0];
	    value = realValue[1];
	    if (key == null) {
		throw new NullPointerException("null key");
	    }
	    if (value == null) {
		throw new NullPointerException("null value for "+key);
	    }
	    return cur;
	}
    }

    class SequenceSet extends AbstractSet {

	// defined by AbstractSet :
	// equals(Object)
	// hashCode()
	// removeAll(Collection)

	SequenceSet() {
	}

	public boolean add(Object o) {
	    throw new UnsupportedOperationException("SequenceSet.add");
	}

	public boolean addAll(Collection c) {
	    throw new UnsupportedOperationException("SequenceSet.addAll");
	}

	public void clear() {
	    SequenceMap.this.clear();
	}

	public boolean contains(Object o) {
	    for (int i=0; i<body.length; i++) {
		if (body[i].equals(o)) { return true; }
	    }
	    return false;
	}

	public boolean containsAll(Collection c) {
	    Iterator i = c.iterator();
	    while (i.hasNext()) {
		if (!contains(i.next())) { return false; }
	    }
	    return true;
	}

	public boolean isEmpty() {
	    return body.length == 0;
	}

	public Iterator iterator() {
	    return (new Iterator() {
		int c = 0;
		boolean removed = true;
		public boolean hasNext() {
		    return (c < body.length);
		}
		public Object next() {
		    if (!hasNext()) {
			throw new NoSuchElementException(""+c);
		    }
		    removed = false;
		    return body[c++];
		}
		public void remove() {
		    if (removed) {
			throw new IllegalStateException(""+c);
		    }
		    SequenceSet.this.remove(body[c-1]);
		}
	    });
	}

	public boolean remove(Object o) {
	    int at = findByKey(((Map.Entry)o).getKey());
	    if (at < 0) { return false; }

	    Map.Entry [] newBody = new Map.Entry[body.length-1];

	    if (at > 0) {
		System.arraycopy(body, 0, newBody, 0, at);
	    }

	    if (at < newBody.length) {
		System.arraycopy(body, at+1, newBody, at,
			newBody.length-at);
	    }

	    body = newBody;
	    return true;
	}

	public boolean removeAll(Collection c) {
	    Iterator i = c.iterator();
	    boolean deleted = false;

	    while (i.hasNext()) {
		deleted = deleted || remove(i.next());
	    }
	    return deleted;
	}

	public boolean retainAll(Collection c) {
	    throw new UnsupportedOperationException("SequenceSet.retainAll");
	}

	public int size() {
	    return body.length;
	}

	public Object [] toArray () {
	    return toArray(new Map.Entry[body.length]);
	}

	public Object [] toArray ( Object [] to ) {

	    if (!(to instanceof Map.Entry[])) {
		throw new ArrayStoreException("Map.Entry required");
	    }

	    if (to.length < body.length) {
		to = (Object[])Array.newInstance(to.getClass().getComponentType(),
			body.length);
	    }

	    System.arraycopy(body, 0, to, 0, body.length);

	    return to;
	}
    }
}
