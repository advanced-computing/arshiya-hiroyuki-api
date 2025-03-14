CREATE TABLE IF NOT EXISTS bus AS 
SELECT * FROM read_csv_auto('bus_2024_2025.csv');

SELECT * FROM bus LIMIT 10;
