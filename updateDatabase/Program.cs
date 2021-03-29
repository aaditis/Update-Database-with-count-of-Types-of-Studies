using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.Data;
using System.IO;

namespace updateDatabase
{
    class Program
    {
        static int Main(string[] args)
        {
            //string databasename = "C:\\SampleDB\\SampleDB1\\DemoPatientsNew.icbd";
            int parameterCount = args.Length;

            // Return an error if proper arguments are not entered
            if (parameterCount != 1)
            {
                Console.WriteLine("Please enter a valid database name.");
                return 0;
            }

            string databasename = args[0];
            // Check if databasename entered is valid or not
            if (!File.Exists(databasename)) 
            {
                Console.WriteLine("Database name entered is incorrect.");
                return 0;
            }

            string connectionString = "Data Source = " + databasename + "; Version = 3; Synchronous = off; Cache Size = 16000; Default Timeout = 120";
            SQLiteConnection con = new SQLiteConnection(connectionString);
            
            // establish connection to the database
            try
            {
                con.Open();
            }
            catch
            {
                Console.WriteLine("Could not establish connection to database");
                return 0;
            }

            //Console.WriteLine("Connection is " + con.State.ToString());

            //CREATE A TEMPORARY TABLES temp1 and temp2
            StringBuilder commandBuilder = new StringBuilder();
            commandBuilder.Append("DROP TABLE IF EXISTS temp1");
            SQLiteCommand cmd = new SQLiteCommand(commandBuilder.ToString(), con);
            int aa = cmd.ExecuteNonQuery();

            commandBuilder = new StringBuilder();
            commandBuilder.Append("DROP TABLE IF EXISTS temp2");
            cmd = new SQLiteCommand(commandBuilder.ToString(), con);
            aa = cmd.ExecuteNonQuery();

            commandBuilder = new StringBuilder();
            commandBuilder.Append("DROP TABLE IF EXISTS StudyTemp");
            cmd = new SQLiteCommand(commandBuilder.ToString(), con);
            aa = cmd.ExecuteNonQuery();

            try
            {
                // Create temporary table 1 -> temp1
                commandBuilder = new StringBuilder();
                commandBuilder.Append("CREATE TABLE temp1 ( ");
                commandBuilder.Append("PatientIndex TEXT, ");
                commandBuilder.Append("StudyUid TEXT)");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                int rc = cmd.ExecuteNonQuery();
                bool execution = false;
                if (rc >= 0)
                {
                    execution = true;
                }

                // Insert data into temp1
                commandBuilder = new StringBuilder();
                commandBuilder.Append("INSERT INTO temp1 ");
                commandBuilder.Append("SELECT PatientIndex, StudyUid ");
                commandBuilder.Append("FROM Series ");
                commandBuilder.Append("GROUP BY StudyUid, PatientIndex");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                rc = cmd.ExecuteNonQuery();
                if (rc >= 0)
                {
                    execution = execution & true;
                }

                // Create temporary table 2 -> temp2
                commandBuilder = new StringBuilder();
                commandBuilder.Append("CREATE TABLE temp2 ( ");
                commandBuilder.Append("PatientIndex TEXT, ");
                commandBuilder.Append("StudyUid TEXT, ");
                commandBuilder.Append("NumberOfSeries_4DMComposite INT)");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                rc = cmd.ExecuteNonQuery();
                if (rc >= 0)
                {
                    execution = execution & true;
                }

                // Insert data into temp2
                commandBuilder = new StringBuilder();
                commandBuilder.Append("INSERT INTO temp2 ");
                commandBuilder.Append("SELECT PatientIndex, StudyUid, ");
                commandBuilder.Append("COUNT(*) ");
                commandBuilder.Append("FROM Series ");
                commandBuilder.Append("where SeriesDescription like '%4dm-composite%' ");
                commandBuilder.Append("GROUP BY StudyUid, PatientIndex");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                rc = cmd.ExecuteNonQuery();
                if (rc >= 0)
                {
                    execution = execution & true;
                }

                // Create temporary table 3 -> StudyTemp
                commandBuilder = new StringBuilder();
                commandBuilder.Append("CREATE TABLE StudyTemp ( ");
                commandBuilder.Append("PatientIndex TEXT, ");
                commandBuilder.Append("StudyUid TEXT, ");
                commandBuilder.Append("NumberOfSeries_4DMComposite INT)");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                rc = cmd.ExecuteNonQuery();
                if (rc >= 0)
                {
                    execution = execution & true;
                }

                // Insert data into StudyTemp
                commandBuilder = new StringBuilder();
                commandBuilder.Append("INSERT INTO StudyTemp ");
                commandBuilder.Append("SELECT temp1.PatientIndex, temp1.StudyUid, temp2.NumberOfSeries_4DMComposite ");
                commandBuilder.Append("FROM temp1 LEFT JOIN temp2 ON temp1.StudyUid = temp2.StudyUid and ");
                commandBuilder.Append("temp1.PatientIndex = temp2.PatientIndex");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                rc = cmd.ExecuteNonQuery();
                if (rc >= 0)
                {
                    execution = execution & true;
                }

                // Update StudyTemp to replace Null with 0 in NumberOfSeries_4DMComposite
                commandBuilder = new StringBuilder();
                commandBuilder.Append("UPDATE StudyTemp ");
                commandBuilder.Append("SET NumberOfSeries_4DMComposite = 0 ");
                commandBuilder.Append("WHERE NumberOfSeries_4DMComposite IS NULL");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                rc = cmd.ExecuteNonQuery();
                if (rc >= 0)
                {
                    execution = execution & true;
                }

                // Add column DataStatus to StudyTemp
                commandBuilder = new StringBuilder();
                commandBuilder.Append("ALTER TABLE StudyTemp ");
                commandBuilder.Append("ADD COLUMN DataStatus TEXT");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                rc = cmd.ExecuteNonQuery();

                // Insert values in column DataStatus in StudyTemp table
                commandBuilder = new StringBuilder();
                commandBuilder.Append("UPDATE StudyTemp ");
                commandBuilder.Append("SET DataStatus = ");
                commandBuilder.Append("CASE WHEN NumberOfSeries_4DMComposite = 0 THEN 'NO' ");
                commandBuilder.Append("WHEN NumberOfSeries_4DMComposite = 1 THEN 'YES' ");
                commandBuilder.Append("ELSE 'YES (' || NumberOfSeries_4DMComposite || ')' END");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                rc = cmd.ExecuteNonQuery();
                if (rc >= 0)
                {
                    execution = execution & true;
                }

                if (execution)
                {
                    Console.WriteLine("Temporary tables created successfully and data inserted in them");
                }

            }
            catch
            {
                Console.WriteLine("Error occured in creating and inserting data in temporary tables ");
                return 0;
            }

            // Add Column 'DataStatus' to Series table
            try
            {
                commandBuilder = new StringBuilder();
                commandBuilder.Append("ALTER TABLE Series ");
                commandBuilder.Append("ADD COLUMN DataStatus TEXT");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                int rc = cmd.ExecuteNonQuery();
            }
            catch
            {
                // Will not exit the application even if it encounters an exception.
                // It will indicate that 'DataStatus' field already exists and
                // proceed to update it with most recent values.
                Console.WriteLine("DataStatus field exists in Series table");
            }

            // Insert 'DataStatus' column from StudyTemp table to Series table
            try
            {
                commandBuilder = new StringBuilder();
                commandBuilder.Append("UPDATE Series ");
                commandBuilder.Append("SET DataStatus = (SELECT StudyTemp.DataStatus from StudyTemp ");
                commandBuilder.Append("WHERE StudyTemp.PatientIndex = Series.PatientIndex ");
                commandBuilder.Append("AND StudyTemp.StudyUid = Series.StudyUid)");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                int rc = cmd.ExecuteNonQuery();
                if (rc >= 0)
                {
                    Console.WriteLine("Series Table updated successfully.");
                }
                // Drop all temporary tables
                commandBuilder = new StringBuilder();
                commandBuilder.Append("DROP TABLE temp1");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                int aa2 = cmd.ExecuteNonQuery();

                commandBuilder = new StringBuilder();
                commandBuilder.Append("DROP TABLE temp2");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                aa2 = cmd.ExecuteNonQuery();

                commandBuilder = new StringBuilder();
                commandBuilder.Append("DROP TABLE StudyTemp");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                aa2 = cmd.ExecuteNonQuery();
            }
            catch
            {
                Console.WriteLine("Failed to update Series table.");
            }
            finally
            {
                // Wait for the user to respond before closing.
                Console.Write("Press any key to continue...");
                Console.ReadKey();
            }
            return 0;
        }
    }
}
