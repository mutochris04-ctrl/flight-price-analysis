-- MySQL staging database.
-- Docker runs this file automatically the first time the MySQL container starts.

CREATE DATABASE IF NOT EXISTS flight_staging;
USE flight_staging;

-- One row per flight record from the CSV.
-- The column types match the original data: text, dates, and money in BDT.
CREATE TABLE IF NOT EXISTS raw_flights (
    id                    INT AUTO_INCREMENT PRIMARY KEY,
    airline               VARCHAR(100),
    source                VARCHAR(10),       -- airport code, e.g. DAC
    source_name           VARCHAR(150),
    destination           VARCHAR(10),
    destination_name      VARCHAR(150),
    departure_date_time   DATETIME,
    arrival_date_time     DATETIME,
    duration_hrs          DECIMAL(6, 2),
    stopovers             VARCHAR(20),
    aircraft_type         VARCHAR(50),
    travel_class          VARCHAR(30),
    booking_source        VARCHAR(50),
    base_fare             DECIMAL(12, 2),    -- BDT
    tax_surcharge         DECIMAL(12, 2),    -- BDT
    total_fare            DECIMAL(12, 2),    -- BDT
    seasonality           VARCHAR(30),
    days_before_departure INT
);

-- Useful checks (run in MySQL Workbench, port 3307):
-- SELECT COUNT(*) FROM raw_flights;
-- SELECT * FROM raw_flights LIMIT 5;
-- SELECT problem, COUNT(*) FROM rejected_flights GROUP BY problem;
