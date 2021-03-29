# Update-Database-with-count-of-Types-of-Studies

The standalone console app aimed at updating the database to a new schema by providing a count of number of studies of each type that are present for each patient. The database has a hierarchical structure – A database has the Patient Info of multiple patients associated with it – Each patient has multiple studies associated with them and each study has multiple series linked with them. 

The console application provides a count of each type of study (or a particular type, as per client needs) for every patient. The database was SQLite, so the SQLite queries to accept inputs (type of study and the database which needs to be updated) from the userwere developed in C#.
