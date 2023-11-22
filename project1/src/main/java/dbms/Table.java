package dbms;
/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.Boolean.*;
import static java.lang.System.arraycopy;
import static java.lang.System.out;

/****************************************************************************************
 * The Table class implements relational database tables (including attribute names, domains
 * and a list of tuples.  Five basic relational algebra operators are provided: project,
 * select, union, minus and join.  The insert data manipulation operator is also provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table
       implements Serializable
{
    /** Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;

    /** Filename extension for database files
     */
    private static final String EXT = ".dbf";

    /** Counter for naming temporary tables.
     */
    private static int count = 0;

    /** Table name.
     */
    private final String name;

    /** Array of attribute names.
     */
    private final String [] attribute;

    /** Array of attribute domains: a domain may be
     *  integer types: Long, Integer, Short, Byte
     *  real types: Double, Float
     *  string types: Character, String
     */
    private final Class [] domain;

    /** Collection of tuples (data storage).
     */
    private final List <Comparable []> tuples;

    /** Primary key (the attributes forming). 
     */
    private final String [] key;

    /** Index into tuples (maps key to tuple).
     */
    private final Map <KeyType, Comparable []> index;

    /** The supported map types.
     */
    private enum MapType { NO_MAP, TREE_MAP, LINHASH_MAP, BPTREE_MAP }

    /** The map type to be used for indices.  Change as needed.
     */
    private static final MapType mType = MapType.LINHASH_MAP;

    /************************************************************************************
     * Make a map (index) given the MapType.
     */
    private static Map <KeyType, Comparable []> makeMap ()
    {
        return switch (mType) {
        case TREE_MAP    -> new TreeMap <> ();
        case LINHASH_MAP -> new LinHashMap <> (KeyType.class, Comparable [].class);
//      case BPTREE_MAP  -> new BpTreeMap <> (KeyType.class, Comparable [].class);
        default          -> null;
        }; // switch
    } // makeMap

    /************************************************************************************
     * Concatenate two arrays of type T to form a new wider array.
     *
     * @see <a href='http://stackoverflow.com/questions/80476/how-to-concatenate-two-arrays-in-java'>StackOverflow Link</a>
     *
     * @param arr1  the first array
     * @param arr2  the second array
     * @return  a wider array containing all the values from arr1 and arr2
     */
    public static <T> T [] concat (T [] arr1, T [] arr2)
    {
        T [] result = Arrays.copyOf (arr1, arr1.length + arr2.length);
        arraycopy (arr2, 0, result, arr1.length, arr2.length);
        return result;
    } // concat



    //-----------------------------------------------------------------------------------
    // Constructors
    //-----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = new ArrayList <> ();
        index     = makeMap ();
    } // primary constructor

    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     *
     * @param _name       the name of the relation
     * @param _attribute  the string containing attributes names
     * @param _domain     the string containing attribute domains (data types)
     * @param _key        the primary key
     * @param _tuples     the list of tuples containing the data
     */  
    public Table (String _name, String [] _attribute, Class [] _domain, String [] _key,
                  List <Comparable []> _tuples)
    {
        name      = _name;
        attribute = _attribute;
        domain    = _domain;
        key       = _key;
        tuples    = _tuples;
        index     = makeMap ();
    } // constructor

    /************************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param _name       the name of the relation
     * @param attributes  the string containing attributes names
     * @param domains     the string containing attribute domains (data types)
     * @param _key        the primary key
     */
    public Table (String _name, String attributes, String domains, String _key)
    {
        this (_name, attributes.split (" "), findClass (domains.split (" ")), _key.split(" "));

        out.println ("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    //----------------------------------------------------------------------------------
    // Public Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @param attributes  the attributes to project onto
     * @return  a table of projected tuples
     */
    public Table project (String attributes)
    {
        out.println ("RA> " + name + ".project (" + attributes + ")");
        var attrs     = attributes.split (" ");
        var colDomain = extractDom (match (attrs), domain);
        var newKey    = (Arrays.asList (attrs).containsAll (Arrays.asList (key))) ? key : attrs;

        List <Comparable []> rows = new ArrayList <> ();

        //  T O   B E   I M P L E M E N T E D 

        int atLen =attrs.length;
// S is the new empty tuple so we will add all values for any attribute mentioned in attrs to S.
        // In order to do the projection.

        for (Comparable[] tuple:tuples) {
            Comparable[] S = new Comparable[atLen];
            int tuLen = attribute.length;

            for (int i = 0; i < atLen; i++) {
                for (int j = 0; j < tuLen; j++) {
                    if (attrs[i].equals(attribute[j])) {
                        //int colNo = j;

                        S[i] = tuple[j];

                        break;
                    }

                }
            }
            // Here we check if the data already exist in rows or not. So it adds just new data to rows and prevent
            // the data redundancy there.
            if (!rows.contains(S)) {
                rows.add(S);
            }
        }
        return new Table (name + count++, attrs, colDomain, newKey, rows);
    } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate  the check condition for tuples
     * @return  a table with tuples satisfying the predicate
     */
    public Table select (Predicate <Comparable []> predicate)
    {
        out.println ("RA> " + name + ".select (" + predicate + ")");

        return new Table (name + count++, attribute, domain, key,
                   tuples.stream ().filter (t -> predicate.test (t))
                                   .collect (Collectors.toList ()));
    } // select

    /**
     * @return Class[] the domain of this object as a Class[]
     */
    public Class[] getDomain() {
        return domain;
    }

    /************************************************************************************
     * Select the tuples satisfying the given simple condition on attributes/constants
     * compared using an operator.
     *
     * #usage movie.select ("year == 1977")
     *
     * @param condition  the check condition as a string for tuples
     * @return  a table with tuples satisfying the condition
     */
    public Table select (String condition)
    {
        out.println ("RA> " + name + ".select (" + condition + ")");

        List <Comparable []> rows = new ArrayList <> ();

        // First Split string by quotations
        // then split string by spaces
        String[] conditionSplit = condition.split(" ");
        // first half of the condition
        String[] tempCondition1 = new String[] {conditionSplit[0]};
        // second half of the condition
        String tempCondition2 = conditionSplit[2];

        // convert to int for comparison purposes if an int
        try {
            int condition2 = Integer.parseInt(String.valueOf(tempCondition2));
            for (var t : tuples) {
                // compare each tuple with the condition and if it passes the condition, add it to the new table
                if (compareWithStringOp(this.extract(t,tempCondition1)[0], conditionSplit[1], condition2)) {
                    rows.add(t);
                } // if
            } // for t
        } catch (NumberFormatException e) { // is not integer
            for (var t : tuples) {
                // compare each tuple with the condition and if it passes the condition, add it to the new table
                if (compareWithStringOp(this.extract(t,tempCondition1)[0], conditionSplit[1], tempCondition2)) {
                    rows.add(t);
                } // if
            } // for t
        }



        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value).  Use an index
     * (Map) to retrieve the tuple with the given key value.  INDEXED SELECT ALGORITHM.
     *
     * @param keyVal  the given key value
     * @return  a table with the tuple satisfying the key predicate
     */
    public Table select (KeyType keyVal)
    {
        out.println ("RA> " + name + ".select (" + keyVal + ")");
        List <Comparable []> rows = new ArrayList <> ();
        try {
            // try to access the tuple with the given keyVal
            Comparable[] temp = this.index.get(keyVal);
            // if it exists, add it to rows
            if (temp != null) {
                out.println("I added a row!");
                rows.add(temp);
            }
            // catch exception if one is given while accessing the tuple
        } catch (Exception e) {
            out.println("Error: " + e + "occurred in select(KeyType keyVal)");
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Union this table and table2.  Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @author Chris Evans
     * @param table2  the rhs table in the union operation
     * @return  a table representing the union
     *
     */
    public Table union (Table table2)
    {
        out.println ("RA> " + name + ".union (" + table2.name + ")");
        if (! compatible (table2)) return null;

        List <Comparable []> rows = new ArrayList <> ();

        // Adding all items of both tables

        for (Comparable[] items: table2.tuples) {
            rows.add(items);
        }

        for(Comparable [] items: this.tuples){
            rows.add(items);
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // union

    /************************************************************************************
     * Take the difference of this table and table2.  Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @param table2  The rhs table in the minus operation
     * @return  a table representing the difference
     */
    public Table minus (Table table2)
    {
        out.println ("RA> " + name + ".minus (" + table2.name + ")");
        // Check to make sure tables are compatible before proceeding
        if (! compatible (table2)) return null;

        // create new list to populate with rows
        List <Comparable []> rows = new ArrayList <> ();

        // Check each tuple in both tables and only if it is in table1 but not table 2 add the currentTuple to the new table
        for (Comparable[] currentTuple: tuples) {
            boolean inTable2 = false;
            for (Comparable[] tuple2: table2.tuples ) {
             if (currentTuple == tuple2) {
                 inTable2 = true;
                 break;
             }
            }
            if (!inTable2) {
                rows.add(currentTuple);
            }
        }

        return new Table (name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by appending "2" to the end of any duplicate attribute name.  Implement using
     * a NESTED LOOP JOIN ALGORITHM.
     *
     * #usage movie.join ("studioName", "name", studio)
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                        + table2.name + ")");

        var t_attrs = attributes1.split (" ");
        var u_attrs = attributes2.split (" ");
        var rows    = new ArrayList <Comparable []> ();
        //Loop through every tuple in both tables and check if they are joinable - if joinable, add to rows
        for (var t : tuples) {
            for (var u : table2.tuples) {
                if (this.joinAble(t, u, t_attrs, u_attrs, table2)) {
                    rows.add(concat(t, u));
                } // if
            } // for u
        } // for t

        //Has to be cloned so that it doesn't overwrite the original table
        String[] tempAttr = table2.attribute.clone();

        //Add 2 to the end of the common attributes
        for(int i = 0; i < table2.attribute.length; i++) {
            var attr = table2.attribute[i];
            for (var attr2 : this.attribute){
                if (attr.equals(attr2))
                    tempAttr[i] = (attr + "2");
            }
        }
        return new Table (name + count++, concat (attribute, tempAttr),
                                          concat (domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * This method is used to check if two tuples are joinable based on a set of attributes
     * @param t The first tuple
     * @param u The second tuple
     * @param t_attrs The attributes of the first tuple in the order they appear in the tuple
     * @param u_attrs The attributes of the second tuple in the order they appear in the tuple
     *                (Note: t_attrs and u_attrs should be the same length)
     * @param table2 The table that the second tuple belongs to
     *
     * @return true if the tuples are joinAble, false otherwise
     */
    public boolean joinAble(Comparable[] t, Comparable[] u, String[] t_attrs, String[] u_attrs, Table table2) {
        //Check if the attributes are the same length
        if(t_attrs.length != u_attrs.length) {
            out.println("Attributes are not the same length");
            return false;
        }
        var tValues = this.extract(t, t_attrs);
        var uValues = table2.extract(u, u_attrs);
        //Loop through the values of the attributes and check if even one is not equal
        for(int i = 0; i < tValues.length; i++) {
            if(tValues[i] != uValues[i]) {
                return false;
            }
        }
        //If it made it to this line of code then all the values must be equal
        return true;
    } // joinAble


    /************************************************************************************
     * Join this table and table2 by performing a "theta-join".  Tuples from both tables
     * are compared attribute1 op attribute2.  Disambiguate attribute names by appending "2"
     * to the end of any duplicate attribute name.  Implement using a Nested Loop Join algorithm.
     *
     * #usage movie.join ("studioName == name", studio)
     *
     * @param condition  the theta join condition
     * @param table2     the rhs table in the join operation
     * @return  a table with tuples satisfying the condition
     */
    public Table join (String condition, Table table2)
    {
        out.println ("RA> " + name + ".join (" + condition + ", " + table2.name + ")");

        var rows = new ArrayList <Comparable []> ();
        String[] conditionSplit = condition.split(" ");
        String[] tempCondition1 = new String[] {conditionSplit[0]};
        String[] tempCondition2 = new String[] {conditionSplit[2]};

        //  T O   B E   I M P L E M E N T E D
        for (var t : tuples) {
            for (var u : table2.tuples) {
                //Run the comparison
                if (compareWithStringOp(this.extract(t,tempCondition1)[0], conditionSplit[1], table2.extract(u,tempCondition2)[0])) {
                    rows.add(concat(t, u));
                } // if
            } // for u
        } // for t

        //Has to be cloned so that it doesn't overwrite the original table
        var tempAttr = table2.attribute.clone();
        //Append 2 to duplicate attributes
        for(int i = 0; i < table2.attribute.length; i++) {
            var attr = table2.attribute[i];
            for (var attr2 : this.attribute){
                if (attr.equals(attr2))
                    tempAttr[i] = (attr + "2");
            }
        }
        return new Table (name + count++, concat (attribute, tempAttr),
                            concat (domain, table2.domain), key, rows);
    } // join


    /************************************************************************************
     * Takes in two comparable values with an operator and returns a boolean
     * value of the comparison of value1 and value2
     *
     *
     * @param value1  the first Comparable value
     * @param value2  the second Comparable value
     * @param op the operator with which to compare value1 and value2
     * @return  a boolean representing the result of value1 op value2
     */
    public boolean compareWithStringOp(Comparable value1, String op, Comparable value2) {
        //OP should be either 1 or 2 chars long.
        // >= <= == > < are the options

        //Look if there is a > or < in the string
        switch (op.charAt(0)) {
            case '>':
                return value1.compareTo(value2) > 0;
            case '<':
                return value1.compareTo(value2) < 0;
        }
        //If there is only one char, then it does not check for equality
        if(op.length() == 1) {
            return  false;
        }
        //If there is a second char, then it checks for equality
        if(op.charAt(1) == '='){
            return value1.equals(value2);
        }
        return  false;
    }
    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above equi-join,
     * but implemented using an INDEXED JOIN ALGORITHM.
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table i_join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", " + table2.name + ")");
        var rows = new ArrayList <Comparable []> ();
        String[] attrs1 = attributes1.split(" ");
        String[] attr2 = attributes2.split(" ");

        //Check if table2 is indexed
        if(table2.index.size() <= 0) {
            out.println("Table 2 is not indexed, running NFL Join instead");
            return this.join(attributes1,attributes2, table2);
        }


        //Loop through all the tuples in table 1
        for (var t : tuples) {
            //The Key of the tuple
            var key = new KeyType(this.extract(t, attrs1));
            //Get the index of the key
            var indexValue = table2.index.get(key);
            //If the indexValue is null, then there is no match
            if(indexValue != null) {
                //They are a match
                var newRow = concat(t, indexValue);
                rows.add(newRow);
                out.println("Match found add new row: ");
                for(var item: newRow){
                    System.out.print(item + " ");
                }
            } // if
            else {
                out.println("No match found");
            }
            //
        } // for t

        //Add 2 to duplicate attribute names

        //Has to be cloned so that it doesn't overwrite the original table
        String[] tempAttr = table2.attribute.clone();
        for(int i = 0; i < table2.attribute.length; i++) {
            var checkAttr = table2.attribute[i];
            for (var checkAttr2 : this.attribute){
                if (checkAttr.equals(checkAttr2))
                    tempAttr[i] = (checkAttr + "2");
            }
        }
        return new Table (name + count++, concat (attribute, tempAttr),
                concat (domain, table2.domain), key, rows);
    } // i_join

    public Object getIndexAt(Object key){
        return index.get(key);
    }
    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using a Hash Join algorithm.
     *
     * @param attributes1  the attributes of this table to be compared (Foreign Key)
     * @param attributes2  the attributes of table2 to be compared (Primary Key)
     * @param table2       the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table h_join (String attributes1, String attributes2, Table table2)
    {

        //  D O   N O T   I M P L E M E N T

        return null;
    } // h_join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join".  Tuples from both tables
     * are compared requiring common attributes to be equal.  The duplicate column is also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *
     * @param table2  the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (Table table2)
    {
        out.println ("RA> " + name + ".join (" + table2.name + ")");

        var rows = new ArrayList <Comparable []> ();

        //Find the common attributes
        StringBuilder CommonAttrBuilder = new StringBuilder();
        for(var attr: attribute) {
            for(var attr2: table2.attribute) {
                if(attr.equals(attr2)) {
                    CommonAttrBuilder.append(attr);
                } //if
            } //for attr2
        } //for attr

        String commonAttrs = CommonAttrBuilder.toString();
        //Perform the join
        Table joinedTable = this.join(commonAttrs, commonAttrs, table2);

        //Build the list of new Attrs and domains
        var newAttrList = new ArrayList<>(Arrays.stream(this.attribute).toList());
        var newDomainList = new ArrayList<>(Arrays.stream(this.domain).toList());

        for(int i = 0; i < table2.attribute.length; i++){
            var attr = table2.attribute[i];
            boolean canAdd = true;
            for(var attr2 : commonAttrs.split(" ")) {
                if (attr.equals(attr2)) {
                    //if a common attribute is found, don't add it
                    canAdd = false;
                } //if
            } // for attr2
            // Add the non-common attributes/domain
            if(canAdd) {
                newAttrList.add(attr);
                newDomainList.add(table2.domain[i]);
            } //if
        } //for attr
        var newAttrsArr = newAttrList.toArray(new String[0]);
        var newDomainArr = newDomainList.toArray(new Class[0]);
        //Remove Duplicate columns
        //Loop through every tuple and extract only the columns needed
        for(var tup : joinedTable.tuples) {
            var newTup = joinedTable.extract(tup, newAttrsArr);
            rows.add(newTup);
        } //for tup
        //Remove the common columns
        return new Table (name + count++, newAttrsArr,
                                          newDomainArr, key, rows);
    } // join

    /************************************************************************************
     * Return the column position for the given attribute name or -1 if not found.
     *
     * @param attr  the given attribute name
     * @return  a column position
     */
    public int col (String attr)
    {
        for (var i = 0; i < attribute.length; i++) {
           if (attr.equals (attribute [i])) return i;
        } // for

        return -1;       // -1 => not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("Star_Wars", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup  the array of attribute values forming the tuple
     * @return  whether insertion was successful
     */
    public boolean insert (Comparable [] tup)
    {
        out.println ("DML> insert into " + name + " values ( " + Arrays.toString (tup) + " )");

        if (typeCheck (tup)) {
            tuples.add (tup);
            var keyVal = new Comparable [key.length];
            var cols   = match (key);
            for (var j = 0; j < keyVal.length; j++) keyVal [j] = tup [cols [j]];
            if (mType != MapType.NO_MAP) index.put (new KeyType (keyVal), tup);
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return  the table's name
     */
    public String getName ()
    {
        return name;
    } // getName

    /************************************************************************************
     * Print this table.
     */
    public void print ()
    {
        out.println ("\n Table " + name);
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
        out.print ("| ");
        for (var a : attribute) out.printf ("%15s", a);
        out.println (" |");
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
        for (var tup : tuples) {
            out.print ("| ");
            for (var attr : tup) out.printf ("%15s", attr);
            out.println (" |");
        } // for
        out.print ("|-");
        out.print ("---------------".repeat (attribute.length));
        out.println ("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex ()
    {
        out.println ("\n Index for " + name);
        out.println ("-------------------");
        if (mType != MapType.NO_MAP) {
            for (var e : index.entrySet ()) {
                out.println (e.getKey () + " -> " + Arrays.toString (e.getValue ()));
            } // for
        } // if
        out.println ("-------------------");
    } // printIndex

    /************************************************************************************
     * Load the table with the given name into memory. 
     *
     * @param name  the name of the table to load
     * @return the table with the name given by String name.
     */
    public static Table load (String name)
    {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream (new FileInputStream (DIR + name + EXT));
            tab = (Table) ois.readObject ();
            ois.close ();
        } catch (IOException ex) {
            out.println ("load: IO Exception");
            ex.printStackTrace ();
        } catch (ClassNotFoundException ex) {
            out.println ("load: Class Not Found Exception");
            ex.printStackTrace ();
        } // try
        return tab;
    } // load

    /************************************************************************************
     * Save this table in a file.
     */
    public void save ()
    {
        try {
            var oos = new ObjectOutputStream (new FileOutputStream (DIR + name + EXT));
            oos.writeObject (this);
            oos.close ();
        } catch (IOException ex) {
            out.println ("save: IO Exception");
            ex.printStackTrace ();
        } // try
    } // save

    //----------------------------------------------------------------------------------
    // Private Methods
    //----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2  the rhs table
     * @return  whether the two tables are compatible
     */
    private boolean compatible (Table table2)
    {
        if (domain.length != table2.domain.length) {
            out.println ("compatible ERROR: table have different arity");
            return false;
        } // if
        for (var j = 0; j < domain.length; j++) {
            if (domain [j] != table2.domain [j]) {
                out.println ("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column  the array of column names
     * @return  an array of column index positions
     */
    private int [] match (String [] column)
    {
        int [] colPos = new int [column.length];

        for (var j = 0; j < column.length; j++) {
            var matched = false;
            for (var k = 0; k < attribute.length; k++) {
                if (column [j].equals (attribute [k])) {
                    matched = true;
                    colPos [j] = k;
                } // for
            } // for
            if ( ! matched) {
                out.println ("match: domain not found for " + column [j]);
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t       the tuple to extract from
     * @param column  the array of column names
     * @return  a smaller tuple extracted from tuple t 
     */
    private Comparable [] extract (Comparable [] t, String [] column)
    {
        var tup    = new Comparable [column.length];
        var colPos = match (column);
        for (var j = 0; j < column.length; j++) tup [j] = t [colPos [j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in array) as well as the type of
     * each value to ensure it is from the right domain. 
     *
     * @param t  the tuple as a array of attribute values
     * @return  whether the tuple has the right size and values that comply
     *          with the given domains
     */
    private boolean typeCheck (Comparable [] t)
    { 
        //  T O   B E   I M P L E M E N T E D 

        return true;      // change once implemented
    } // typeCheck

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className  the array of class name (e.g., {"Integer", "String"})
     * @return  an array of Java classes
     */
    private static Class [] findClass (String [] className)
    {
        var classArray = new Class [className.length];

        for (var i = 0; i < className.length; i++) {
            try {
                classArray [i] = Class.forName ("java.lang." + className [i]);
            } catch (ClassNotFoundException ex) {
                out.println ("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos  the column positions to extract.
     * @param group   where to extract from
     * @return  the extracted domains
     */
    private Class [] extractDom (int [] colPos, Class [] group)
    {
        var obj = new Class [colPos.length];

        for (var j = 0; j < colPos.length; j++) {
            obj [j] = group [colPos [j]];
        } // for

        return obj;
    } // extractDom
} // Table class

