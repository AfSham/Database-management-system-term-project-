package dbms;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static java.lang.System.out;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Class to test the Table implementation for Project 2.
 */
class TableTest {

    /**
     * Successfully create valid tables and perform union, selection, join, minus,
     * and projection operations without any errors.
     */
    @Test
    void full_test_p2() {}



    @Test
    /**
     * Create a valid table of movies with the indexed join.
     */
    void testIndex() {
        //Create table
        var movies = new Table ("movie", "title year length genre studioName producerNo",
                "String Integer Integer String String Integer", "title year");

        var actors = new Table("Actors", "actorId name title year", "Integer String String Integer", "actorId");

        var actor0 = new Comparable [] { 1, "John", "Star_Wars", 1977 };
        var actor1 = new Comparable [] { 2, "John", "Star_Wars_2", 1980 };
        var actor2 = new Comparable [] { 3, "James", "Rocky", 1985 };
        var actor3 = new Comparable [] {4, "Bob", "Star_Wars", 1977};

        actors.insert(actor0);
        actors.insert(actor1);
        actors.insert(actor2);
        actors.insert(actor3);

        //Fill table
        Comparable[][] filmsToAdd = new Comparable[8][6];
        filmsToAdd[0] = new Comparable[] { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
        filmsToAdd[1] = new Comparable[] { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
        filmsToAdd[2] = new Comparable[] { "Rocky", 1985, 200, "action", "Universal", 12125 };
        filmsToAdd[3] = new Comparable[] { "Rambo", 1978, 100, "action", "Universal", 32355 };
        filmsToAdd[4] = new Comparable[] { "Fantastic Mr. Fox", 2009, 124, "drama", "Fox", 12345 };
        filmsToAdd[5] = new Comparable[] { "Coraline", 2009, 124, "drama", "Fox", 12345 };
        filmsToAdd[6] = new Comparable[] { "500 Days of Summer", 2009, 200, "romance", "Universal", 12125 };
        filmsToAdd[7] = new Comparable[] { "Twilight", 2009, 100, "romance", "Universal", 32355 };


        for (Comparable[] film : filmsToAdd) {
            movies.insert(film);
        }


        //Test i_join
        var joined = actors.i_join("title year", "title year", movies);
        joined.print();
        //Test index
        //movies.printIndex();
        //out.println(movies.getIndexAt(new KeyType(new Comparable[] {"Star_Wars", 1977})));

    }


    @Test
    void i_select() {
        //Create table
        var movies2 = new Table ("movie", "title year length genre studioName producerNo",
                "String Integer Integer String String Integer", "title year");

        //Fill table
        Comparable[][] filmsToAdd = new Comparable[8][6];
        filmsToAdd[0] = new Comparable[] { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
        filmsToAdd[1] = new Comparable[] { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
        filmsToAdd[2] = new Comparable[] { "Rocky", 1985, 200, "action", "Universal", 12125 };
        filmsToAdd[3] = new Comparable[] { "Rambo", 1978, 100, "action", "Universal", 32355 };
        filmsToAdd[4] = new Comparable[] { "Fantastic_Mr._Fox", 2009, 124, "drama", "Fox", 12345 };
        filmsToAdd[5] = new Comparable[] { "Coraline", 2009, 124, "drama", "Fox", 12345 };
        filmsToAdd[6] = new Comparable[] { "500_Days_of_Summer", 2009, 200, "romance", "Universal", 12125 };
        filmsToAdd[7] = new Comparable[] { "Twilight", 2009, 100, "romance", "Universal", 32355 };

        for (Comparable[] film : filmsToAdd) {
            movies2.insert(film);
        }

        movies2.printIndex();

        //Test i_select
        Table t_iselect = movies2.select (new KeyType ("Twilight",2009));
        t_iselect.print ();

        // Test not there wont crash
        t_iselect = movies2.select(new KeyType ("500_Days_Of_Summer",2004));
        t_iselect.print ();

        // Test partial won't return
        t_iselect = movies2.select(new KeyType ("Star_Wars"));
        t_iselect.print ();

        var actors = new Table("Actors", "actorId name title year", "Integer String String Integer", "actorId");

        var actor0 = new Comparable [] { 1, "John", "Star_Wars", 1977 };
        var actor1 = new Comparable [] { 2, "John", "Star_Wars_2", 1980 };
        var actor2 = new Comparable [] { 3, "James", "Rocky", 1985 };

        actors.insert(actor0);
        actors.insert(actor1);
        actors.insert(actor2);

        // This should return 2 tuples
        Table a_iselect = actors.select(new KeyType(1));
        a_iselect.print ();

    }
//
//    /**
//     * Create 2 valid tables of movies and perform the union operation.
//     */
//    @Test
//    void union() {
//        var movie = new Table ("movie", "title year length genre studioName producerNo",
//                "String Integer Integer String String Integer", "title year");
//        var film0 = new Comparable [] { "Star", 1977, 124, "sciFi", "Fox", 12345 };
//        var film1 = new Comparable [] { "Star2", 1980, 124, "sciFi", "Fox", 12345 };
//        var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
//        var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };
//        movie.insert (film0);
//        movie.insert (film1);
//        movie.insert (film2);
//        movie.insert (film3);
//
//        var actors = new Table("Actors", "actorId name title", "Integer String String", "actorId");
//        var actor0 = new Comparable [] { 1, "John", "Star" };
//        var actor1 = new Comparable [] { 2, "John", "Star2" };
//        var actor2 = new Comparable [] { 3, "James", "Rocky" };
//        var actor3 = new Comparable [] {4, "Bob", "Star"};
//
//        actors.insert(actor0);
//        actors.insert(actor1);
//        actors.insert(actor2);
//        actors.insert(actor3);
//        var joined = movie.join(actors);
//        joined.print();
//        for(var dom : joined.getDomain()){
//            out.println(dom);
//        }
//        actors.print();
//        assertTrue(true);
//
//    }
//    /**
//     * Create a valid table of movies and perform join operation without condition.
//     */
//    @Test
//    void testNaturaljoin() {
//        var movie = new Table ("movie", "title year length genre studioName producerNo",
//                "String Integer Integer String String Integer", "title year");
//        var film0 = new Comparable [] { "Star", 1977, 124, "sciFi", "Fox", 12345 };
//        var film1 = new Comparable [] { "Star2", 1980, 124, "sciFi", "Fox", 12345 };
//        var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
//        var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };
//        movie.insert (film0);
//        movie.insert (film1);
//        movie.insert (film2);
//        movie.insert (film3);
//
//        var actors = new Table("Actors", "actorId name title", "Integer String String", "actorId");
//        var actor0 = new Comparable [] { 1, "John", "Star" };
//        var actor1 = new Comparable [] { 2, "John", "Star2" };
//        var actor2 = new Comparable [] { 3, "James", "Rocky" };
//        var actor3 = new Comparable [] {4, "Bob", "Star"};
//
//        actors.insert(actor0);
//        actors.insert(actor1);
//        actors.insert(actor2);
//        actors.insert(actor3);
//        var joined = movie.join(actors);
//        joined.print();
//        for(var dom : joined.getDomain()){
//            out.println(dom);
//        }
//        actors.print();
//        assertTrue(true);
//
//    }
///**
// * Test Attribute join function.
// */
//    @Test
//    void testAttributeJoin(){
//        var movie = new Table ("movie", "title year length genre studioName producerNo",
//                "String Integer Integer String String Integer", "title year");
//        var film0 = new Comparable [] { "Star", 1977, 124, "sciFi", "Fox", 12345 };
//        var film1 = new Comparable [] { "Star2", 1980, 124, "sciFi", "Fox", 12345 };
//        var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
//        var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };
//        movie.insert (film0);
//        movie.insert (film1);
//        movie.insert (film2);
//        movie.insert (film3);
//
//        var actors = new Table("Actors", "actorId name movieTitle", "Integer String String", "actorId");
//        var actor0 = new Comparable [] { 1, "John", "Star" };
//        var actor1 = new Comparable [] { 2, "John", "Star2" };
//        var actor2 = new Comparable [] { 3, "James", "Rocky" };
//        var actor3 = new Comparable [] {4, "Bob", "Star"};
//
//        actors.insert(actor0);
//        actors.insert(actor1);
//        actors.insert(actor2);
//        actors.insert(actor3);
//        var joined = movie.join("title", "movieTitle", actors);
//        joined.print();
//        for(var dom : joined.getDomain()){
//            out.println(dom);
//        }
//        assertTrue(true);
//
//    }
//
//    /**
//     * Create a valid table of movies and perform join operation with specified condition.
//     * and be persisted without any errors.
//     */
//    @Test
//    void testJoin() {
//        var movie = new Table ("movie", "title year length genre studioName producerNo",
//                "String Integer Integer String String Integer", "title year");
//        var film0 = new Comparable [] { "Star", 1977, 124, "sciFi", "Fox", 12345 };
//        var film1 = new Comparable [] { "Star2", 1980, 124, "sciFi", "Fox", 12345 };
//        var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
//        var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };
//        movie.insert (film0);
//        movie.insert (film1);
//        movie.insert (film2);
//        movie.insert (film3);
//
//        var actors = new Table("Actors", "actorId name title", "Integer String String", "actorId");
//        var actor0 = new Comparable [] { 1, "John", "Star" };
//        var actor1 = new Comparable [] { 2, "John", "Star2" };
//        var actor2 = new Comparable [] { 3, "James", "Rocky" };
//        var actor3 = new Comparable [] {4, "Bob", "Star"};
//
//        actors.insert(actor0);
//        actors.insert(actor1);
//        actors.insert(actor2);
//        actors.insert(actor3);
//        var joined = movie.join("title < title",actors);
//        joined.print();
//        for(var dom : joined.getDomain()){
//            out.println(dom);
//        }
//        assertTrue(true);
//    }
//
//    /**
//     * Create a valid table of movies with the join
//     */
//    @Test
//    void testEquiJoin() {
//        var movie = new Table("movie", "title year length genre studioName producerNo",
//                "String Integer Integer String String Integer", "title year");
//
//        var cinema = new Table ("cinema", "movieTitle year length genre city storeNo",
//                "String Integer Integer String String Integer", "movieTitle year");
//
//        var film0 = new Comparable [] { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
//        var film1 = new Comparable [] { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
//        var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
//        var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };
//
//        out.println ();
//        movie.insert (film0);
//        movie.insert (film1);
//        movie.insert (film2);
//        movie.insert (film3);
//
//
//        var film4 = new Comparable [] { "Star_Wars", 1977, 124, "sciFi", "Athens", 12389 };
//        var film5 = new Comparable [] { "Fantastic_Mr_Fox", 1980, 124, "sciFi", "Atlanta", 12389 };
//        var film6 = new Comparable [] { "Rocky", 1985, 200, "action", "Orlando", 12885 };
//        var film7 = new Comparable [] { "Rambo", 1978, 100, "action", "Los_Angeles", 32345 };
//
//        out.println ();
//        cinema.insert (film4);
//        cinema.insert (film5);
//        cinema.insert (film6);
//        cinema.insert (film7);
//
//        var joined = cinema.join("movieTitle", "title", movie);
//        joined.print();
//
//    }
//
//
//    /**
//     * Create a valid table of movies with the difference operation.
//     */
//    @Test
//    void testDifference() {
//        var movie = new Table("movie", "title year length genre studioName producerNo",
//                "String Integer Integer String String Integer", "title year");
//
//        var cinema = new Table ("cinema", "title year length genre studioName producerNo",
//                "String Integer Integer String String Integer", "title year");
//
//        var film0 = new Comparable [] { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
//        var film1 = new Comparable [] { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
//        var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
//        var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };
//
//        out.println ();
//        movie.insert (film0);
//        movie.insert (film1);
//        movie.insert (film2);
//        movie.insert (film3);
//
//
//        var film4 = new Comparable [] { "Galaxy_Quest", 1999, 104, "comedy", "DreamWorks", 67890 };
//        out.println ();
//        cinema.insert (film2);
//        cinema.insert (film3);
//        cinema.insert (film4);
//
//        cinema.print ();
//        out.println ();
//
//        var t_minus = movie.minus (cinema);
//        t_minus.print ();
//    }
//
//    /**
//     * Create a valid table of movies with the projection operation.
//     */
//    @Test
//    void testProjection() {
//        var movie = new Table ("movie", "title year length genre studioName producerNo",
//                "String Integer Integer String String Integer", "title year");
//        var film0 = new Comparable [] { "Star", 1977, 124, "sciFi", "Fox", 12345 };
//        var film1 = new Comparable [] { "Star2", 1980, 124, "sciFi", "Fox", 12345 };
//        var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
//        var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };
//        movie.insert (film0);
//        movie.insert (film1);
//        movie.insert (film2);
//        movie.insert (film3);
//
//
//        //--------------------- project: title year
//
//        out.println ();
//        var t_project = movie.project ("title year");
//        t_project.print ();
//
//        //--------------------- project: title year
//        out.println();
//        var t_project_2 = movie.project("year length");
//        t_project_2.print();
//    }
//
//    /**
//     * Create a valid table that will be the result of tuples that match a given string condition.
//     */
//    @Test
//    void testSelect1() {
//        var movie = new Table ("movie", "title year length genre studioName producerNo",
//                "String Integer Integer String String Integer", "title year");
//
//        var film0 = new Comparable [] { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
//        var film1 = new Comparable [] { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
//        var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
//        var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };
//        var film4 = new Comparable [] { "Fantastic Mr. Fox", 2009, 124, "drama", "Fox", 12345 };
//        var film5 = new Comparable [] { "Coraline", 2009, 124, "drama", "Fox", 12345 };
//        var film6 = new Comparable [] { "500 Days of Summer", 2009, 200, "romance", "Universal", 12125 };
//        var film7 = new Comparable [] { "Twilight", 2009, 100, "romance", "Universal", 32355 };
//
//        out.println ();
//        movie.insert (film0);
//        movie.insert (film1);
//        movie.insert (film2);
//        movie.insert (film3);
//        movie.insert (film4);
//        movie.insert (film5);
//        movie.insert (film6);
//        movie.insert (film7);
//        movie.print ();
//
//
//        //--------------------- select: equals, &&
//
//        out.println ();
//        var t_select = movie.select ("year > 2000");
//        t_select.print ();
//
//        //--------------------- select: <
//
//        out.println ();
//        var t_select2 = movie.select ("title == Star_Wars");
//        t_select2.print ();
//
//    }
//
//*/

    

    // /**
    //  * Create a valid table of movies with the difference operation.
    //  */
    // @Test
    // void testDifference() {
    //     var movie = new Table("movie", "title year length genre studioName producerNo",
    //             "String Integer Integer String String Integer", "title year");

    //     var cinema = new Table ("cinema", "title year length genre studioName producerNo",
    //             "String Integer Integer String String Integer", "title year");

    //     var film0 = new Comparable [] { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
    //     var film1 = new Comparable [] { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
    //     var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
    //     var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };

    //     out.println ();
    //     movie.insert (film0);
    //     movie.insert (film1);
    //     movie.insert (film2);
    //     movie.insert (film3);


    //     var film4 = new Comparable [] { "Galaxy_Quest", 1999, 104, "comedy", "DreamWorks", 67890 };
    //     out.println ();
    //     cinema.insert (film2);
    //     cinema.insert (film3);
    //     cinema.insert (film4);

    //     cinema.print ();
    //     out.println ();

    //     var t_minus = movie.minus (cinema);
    //     t_minus.print ();
    // }

    // /**
    //  * Create a valid table of movies with the projection operation.
    //  */
    // @Test
    // void testProjection() {
    //     var movie = new Table ("movie", "title year length genre studioName producerNo",
    //             "String Integer Integer String String Integer", "title year");
    //     var film0 = new Comparable [] { "Star", 1977, 124, "sciFi", "Fox", 12345 };
    //     var film1 = new Comparable [] { "Star2", 1980, 124, "sciFi", "Fox", 12345 };
    //     var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
    //     var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };
    //     movie.insert (film0);
    //     movie.insert (film1);
    //     movie.insert (film2);
    //     movie.insert (film3);


    //     //--------------------- project: title year

    //     out.println ();
    //     var t_project = movie.project ("title year");
    //     t_project.print ();

    //     //--------------------- project: title year
    //     out.println();
    //     var t_project_2 = movie.project("year length");
    //     t_project_2.print();
    // }

    // /**
    //  * Create a valid table that will be the result of tuples that match a given string condition.
    //  */
    // @Test
    // void testSelect1() {
    //     var movie = new Table ("movie", "title year length genre studioName producerNo",
    //             "String Integer Integer String String Integer", "title year");

    //     var film0 = new Comparable [] { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
    //     var film1 = new Comparable [] { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
    //     var film2 = new Comparable [] { "Rocky", 1985, 200, "action", "Universal", 12125 };
    //     var film3 = new Comparable [] { "Rambo", 1978, 100, "action", "Universal", 32355 };
    //     var film4 = new Comparable [] { "Fantastic Mr. Fox", 2009, 124, "drama", "Fox", 12345 };
    //     var film5 = new Comparable [] { "Coraline", 2009, 124, "drama", "Fox", 12345 };
    //     var film6 = new Comparable [] { "500 Days of Summer", 2009, 200, "romance", "Universal", 12125 };
    //     var film7 = new Comparable [] { "Twilight", 2009, 100, "romance", "Universal", 32355 };

    //     out.println ();
    //     movie.insert (film0);
    //     movie.insert (film1);
    //     movie.insert (film2);
    //     movie.insert (film3);
    //     movie.insert (film4);
    //     movie.insert (film5);
    //     movie.insert (film6);
    //     movie.insert (film7);
    //     movie.print ();


    //     //--------------------- select: equals, &&

    //     out.println ();
    //     var t_select = movie.select ("year > 2000");
    //     t_select.print ();

    //     //--------------------- select: <

    //     out.println ();
    //     var t_select2 = movie.select ("title == Star_Wars");
    //     t_select2.print ();

    // }

    // @Test
    // void testSplit() {
    //     String number_of_keys = "40";
    //     LinHashMap.main(new String[]{number_of_keys});

    //     assertTrue(true);
    // }

    // @Test
    // void testsplit2() {
    //     var totalKeys = 40;
    //     var RANDOMLY  = true;

    //     LinHashMap <Integer, Integer> ht = new LinHashMap <> (Integer.class, Integer.class);

    //     if (RANDOMLY) {
    //         var rng = new Random();
    //         for (var i = 1; i <= totalKeys; i += 2){
    //             int i_i = rng.nextInt(totalKeys);
    //             ht.put (rng.nextInt (2 * totalKeys), i * i_i);
    //         }
    //     } else {
    //         for (var i = 1; i <= totalKeys; i += 2) ht.put (i, i * i);
    //     } // if

    //     ht.print ();
    //     for (var i = 0; i <= totalKeys; i++) {
    //         out.println ("key = " + i + " value = " + ht.get (i));
    //     } // for
    //     out.println ("-------------------------------------------");
    //     //out.println ("Average number of buckets accessed = " + ht.count / (double) totalKeys);

    //     assertTrue(true);
    // }
}
