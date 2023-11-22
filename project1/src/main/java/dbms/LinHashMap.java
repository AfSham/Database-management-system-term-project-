package dbms;
/************************************************************************************
 * @file LinHashMap.java
 *
 * @author  John Miller
 */

import java.io.*;
import java.lang.reflect.Array;
import static java.lang.System.out;
import java.util.*;

/************************************************************************************
 * This class provides hash maps that use the Linear Hashing algorithm.
 * A hash table is created that is an expandable array-list of buckets.
 */
public class LinHashMap <K, V>
       extends AbstractMap <K, V>
       implements Serializable, Cloneable, Map <K, V>
{
    /** The debug flag
     */
    private static final boolean DEBUG = true;

    /** The number of slots (for key-value pairs) per bucket.
     */
    private static final int SLOTS = 4;

    /** The threshold/upper bound on the load factor
     */
    private static final double THRESHOLD = .75;

    /** The class for type K.
     */
    private final Class <K> classK;

    /** The class for type V.
     */
    private final Class <V> classV;

    /********************************************************************************
     * This inner class defines buckets that are stored in the hash table.
     */
    private class Bucket
    {
        int    nKeys;
        K []   key;
        V []   value;
        Bucket next;

        @SuppressWarnings("unchecked")
        Bucket ()
        {
            nKeys = 0;
            key   = (K []) Array.newInstance (classK, SLOTS);
            value = (V []) Array.newInstance (classV, SLOTS);
            next  = null;
        } // constructor

        void remove(int i) {
            for(int j = i; j < nKeys - 1; j++) {
                key[j] = key[j+1];
                value[j] = value[j+1];
            }
            nKeys--;
        }
        V find (K k)
        {
            for (var j = 0; j < nKeys; j++) if (key[j].equals (k)) return value[j];
            return null;
        } // find

        void add (K k, V v)
        {
            key[nKeys]   = k;
            value[nKeys] = v;
            nKeys++;
        } // add

        void print ()
        {
            out.print ("[ " );
            for (var j = 0; j < nKeys; j++) out.print (key[j] + " . ");
            out.println ("]" );
        } // print

    } // Bucket inner class

    /** The list of buckets making up the hash table.
     */
    private final List <Bucket> hTable;

    /** The modulus for low resolution hashing
     */
    private int mod1;

    /** The modulus for high resolution hashing
     */
    private int mod2;

    /** The index of the next bucket to split.
     */
    private int isplit = 0;

    /** Counter for the number buckets accessed (for performance testing).
     */
    private int count = 0;

    /** The counter for the total number of keys in the LinHash Map
     */
    private int keyCount = 0;

    /********************************************************************************
     * Construct a hash table that uses Linear Hashing.
     * @param _classK  the class for keys (K)
     * @param _classV  the class for values (V)
     * @author Afsaneh Shams
     */
    public LinHashMap (Class <K> _classK, Class <V> _classV)
    {
        classK = _classK;
        classV = _classV;
        mod1   = 4;                                                          // initial size
        mod2   = 2 * mod1;
        hTable = new ArrayList <> ();
        for (var i = 0; i < mod1; i++) hTable.add (new Bucket ());
    } // constructor

    /********************************************************************************
     * Return a set containing all the entries as pairs of keys and values.
     * @return  the set view of the map
     */
    public Set <Map.Entry <K, V>> entrySet ()
    {
        var enSet = new HashSet <Map.Entry <K, V>> ();

        //we need to know the number of buckets in the hash table to go over each bucket and then for each bucket
        // find the number of keys and values there.
        int numBuck=hTable.size();

        //here we iterate over the buckets available in the hash table and for each bucket, get its value using the
        // Bucket class and get method.
        for (int l=0; l<numBuck; l++){
        Bucket buck = hTable.get(l);
        //In this part we calculate the number of keys exist in the bucket. So we use buck which has the value for
        // each bucket of the hash table
        int numKeys=buck.nKeys;

        //Now for each key we go over the key and its value and add it to our set.
        for (int i=0; i<numKeys; i++){

            //here we get the key and its value for each key in a bucket. and save them in bucketKey and
            // bucketValue respectively.
            K bucketKey = buck.key[i];
            V bucketValue = buck.value[i];

            //here we add the key (bucketkey) and value (bucketvalue) to our set which is called the enSet.
            enSet.add(Map.entry(bucketKey, bucketValue));

            }
        }

        return enSet;
    } // entrySet

    /********************************************************************************
     * Given the key, look up the value in the hash table.
     * @param key  the key used for look up
     * @return  the value associated with the key
     */
    @SuppressWarnings("unchecked")
    public V get (Object key)
    {

        var i = h (key);
        var i2 = h2 (key);
        var hReturn = find ((K) key, hTable.get (i), true);
        if(hReturn == null) {
            hReturn = find ((K) key, hTable.get (i2), true);
        }
        return hReturn;
    } // get

    /********************************************************************************
     * Put the key-value pair in the hash table.  Split the 'isplit' bucket chain
     * when the load factor is exceeded.
     * @param key    the key to insert
     * @param value  the value to insert
     * @return  the old/previous value, null if none
     */
    public V put (K key, V value)
    {
        var i    = h (key);                                                  // hash to i-th bucket chain
        var bh   = hTable.get (i);                                           // start with home bucket
        var oldV = find (key, bh, false);                                    // find old value associated with key
        out.println ("LinearHashMap.put: key = " + key + ", h() = " + i + ", value = " + value);

        keyCount++;                                                          // increment the key count
        var lf = loadFactor ();                                              // compute the load factor
        if (DEBUG) out.println ("put: load factor = " + lf);
        if (lf > THRESHOLD) split ();                                        // split beyond THRESHOLD

        var b = bh;
        while (true) {
            if (b.nKeys < SLOTS) { b.add (key, value); return oldV; }
            if (b.next != null) b = b.next; else break;
        } // while

        var bn = new Bucket ();
        bn.add (key, value);
        b.next = bn;                                                         // add new bucket at end of chain
        return oldV;
    } // put

    /********************************************************************************
     * Print the hash table.
     */
    public void print ()
    {
        out.println ("LinHashMap");
        out.println ("-------------------------------------------");

        for (var i = 0; i < hTable.size (); i++) {
            out.print ("Bucket [ " + i + " ] = ");
            var j = 0;
            for (var b = hTable.get (i); b != null; b = b.next) {
                if (j > 0) out.print (" \t\t --> ");
                b.print ();
                j++;
            } // for
        } // for

        out.println ("-------------------------------------------");
    } // print
 
    /********************************************************************************
     * Return the size (SLOTS * number of home buckets) of the hash table. 
     * @return  the size of the hash table
     */
    public int size ()
    {
        return SLOTS * (mod1 + isplit);
    } // size

//    /********************************************************************************
//     * Split bucket chain 'isplit' by creating a new bucket chain at the end of the
//     * hash table and redistributing the keys according to the high resolution hash
//     * function 'h2'.  Increment 'isplit'.  If current split phase is complete,
//     * reset 'isplit' to zero, and update the hash functions.
//     *
//     * @author Christopher Evans
//     * @author Razvan Beldanu
//     */
/*
    private void split ()
    {
        out.println ("split: bucket chain " + isplit);

        //  Add a new bucket to the end of the hash table
        Bucket splitBucket = hTable.get(isplit);
        Bucket newBucket = new Bucket();
        hTable.add(newBucket);


//          Iterate through the bucket chain and redistribute the keys
        for (Bucket b = splitBucket; b != null; b = b.next) {
            for (int i = 0; i < b.nKeys; i++) {
                K key = b.key[i];
                V value = b.value[i];
                int index = h2(key);
                if (index > h(key)) {
                    //Add the key and value to the new bucket
                    newBucket.add(key, value);
                    //Remove the key and value from the old bucket
                    b.remove(i);
                }
            }
        }

        //  Increment the split counter
        isplit++;

        //  If the split counter is equal to the number of buckets, reset the counter
        if (isplit == mod1) {
            isplit = 0;
            mod1 = mod2;
            mod2 = 2 * mod1;
        }


    } // split*/


    /********************************************************************************
     * Return the load factor for the hash table.
     * @return  the load factor
     */
    private double loadFactor ()
    {
        return keyCount / (double) size ();
    } // loadFactor

    /********************************************************************************
     * Find the key in the bucket chain that starts with home bucket bh.
     * @param key     the key to find
     * @param bh      the given home bucket
     * @param by_get  whether 'find' is called from 'get' (performance monitored)
     * @return  the current value stored for the key
     */
    private V find (K key, Bucket bh, boolean by_get)
    {
        for (var b = bh; b != null; b = b.next) {
            if (by_get) count++;
            V result = b.find (key);
            if (result != null) return result;
        } // for
        return null;
    } // find

    /********************************************************************************
     * Hash the key using the low resolution hash function.
     * @param key  the key to hash
     * @return  the location of the bucket chain containing the key-value pair
     */
    private int h (Object key)
    {
        return Math.abs(key.hashCode () % mod1);

    } // h

    /********************************************************************************
     * Hash the key using the high resolution hash function.
     * @param key  the key to hash
     * @return  the location of the bucket chain containing the key-value pair
     */
    private int h2 (Object key)
    {
        return key.hashCode () % mod2;
    } // h2

    /********************************************************************************
     * The main method used for testing.
     * @param  args the command-line arguments (args [0] gives number of keys to insert)
     */
    public static void main (String [] args)
    {
        var totalKeys = 15;
        var RANDOMLY  = false;

        LinHashMap <Integer, Integer> ht = new LinHashMap <> (Integer.class, Integer.class);
        if (args.length == 1) totalKeys = Integer.valueOf (args [0]);

        if (RANDOMLY) {
            var rng = new Random ();
            for (var i = 1; i <= totalKeys; i += 1) ht.put (rng.nextInt (2 * totalKeys), i * i);
        } else {
            for (var i = 1; i <= totalKeys; i += 1) ht.put (i, i * i);
        } // if

        ht.print ();
        for (var i = 0; i <= totalKeys; i++) {
            out.println ("key = " + i + " value = " + ht.get (i));
        } // for
        out.println ("-------------------------------------------");
        out.println ("Average number of buckets accessed = " + ht.count / (double) totalKeys);
    } // main

} // LinHashMap class

