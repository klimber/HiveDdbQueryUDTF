package com.klimber.hiveddbudtf;

import com.klarna.hiverunner.HiveRunnerExtension;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HiveRunnerExtension.class)
public class HelloHiveRunnerTest {

    @HiveSQL(files = {})
    private HiveShell shell;

    @BeforeEach
    public void setupSourceDatabase() {
        this.shell.execute("CREATE DATABASE source_db");
        this.shell.execute("CREATE TABLE source_db.test_table ("
                           + "year STRING, value INT"
                           + ")");

        this.shell.execute(Paths.get("src/test/resources/HelloHiveRunnerTest/create_max.sql"));
    }

    @Test
    public void testMaxValueByYear() {
        /*
         * Insert some source data
         */
        this.shell.insertInto("source_db", "test_table")
                  .withColumns("year", "value")
                  .addRow("2014", 3)
                  .addRow("2014", 4)
                  .addRow("2015", 2)
                  .addRow("2015", 5)
                  .commit();

        /*
         * Execute the query
         */
        this.shell.execute(Paths.get("src/test/resources/HelloHiveRunnerTest/calculate_max.sql"));

        /*
         * Verify the result
         */
        List<Object[]> result = this.shell.executeStatement("select * from my_schema.result");

        Assertions.assertEquals(2, result.size());
        Assertions.assertArrayEquals(new Object[]{"2014",4}, result.get(0));
        Assertions.assertArrayEquals(new Object[]{"2015",5}, result.get(1));
    }
    
}
