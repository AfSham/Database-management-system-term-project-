# Steps to run project
1. Cd into project1
2. Compile and generate jar file: $ mvn clean package site
  Note: this has MovieDB.java as the main class for the jar file.
  To use a different file as main class:
    a. Go into pom.xml
    b. In the following code (lines 82-92), change dbms.MovieDB (line 88) to dbms.<name of file with main function you want>:
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-jar-plugin</artifactId>
          <configuration>
            <archive>
              <manifest>
                <mainClass>dbms.MovieDB</mainClass>
              </manifest>
            </archive>
          </configuration>
        </plugin>
3. To run: $java -jar target/
4. To run using our tests (within the src/test/TableTest.java): $ mvn test
5. To view documentation: run $mvn site:run
    This will take you to the overall project documentation page. If you wish to view the javadocs, click the "Project Reports" toggle in the lefthand menu and click JavaDoc from there.
