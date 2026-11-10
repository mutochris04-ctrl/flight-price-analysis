-- PostgreSQL analytics database (flight_analytics).
-- Docker runs this file automatically the first time the PostgreSQL container starts.

-- Clean and enriched flight records
CREATE TABLE IF NOT EXISTS flights (
    id                    INT PRIMARY KEY,
    airline               VARCHAR(100) NOT NULL,
    source                VARCHAR(10)  NOT NULL,
    source_name           VARCHAR(150),
    destination           VARCHAR(10)  NOT NULL,
    destination_name      VARCHAR(150),
    departure_date_time   TIMESTAMP,
    arrival_date_time     TIMESTAMP,
    duration_hrs          NUMERIC(6, 2),
    stopovers             VARCHAR(20),
    aircraft_type         VARCHAR(50),
    travel_class          VARCHAR(30),
    booking_source        VARCHAR(50),
    base_fare             NUMERIC(12, 2) NOT NULL,
    tax_surcharge         NUMERIC(12, 2) NOT NULL,
    total_fare            NUMERIC(12, 2) NOT NULL,
    seasonality           VARCHAR(30),
    days_before_departure INT,
    calculated_total_fare NUMERIC(12, 2),   -- base_fare + tax_surcharge
    fare_mismatch         BOOLEAN,          -- TRUE if total_fare is different from the calculation
    route                 VARCHAR(30),      -- e.g. DAC-CXB
    season_type           VARCHAR(10)       -- Peak or Non-Peak
);

-- KPI 1 and 3: average fare and booking count by airline
CREATE TABLE IF NOT EXISTS kpi_airline (
    airline        VARCHAR(100) PRIMARY KEY,
    booking_count  INT,
    avg_total_fare NUMERIC(12, 2)
);

-- KPI 2a: average fare for each season
CREATE TABLE IF NOT EXISTS kpi_season (
    seasonality         VARCHAR(30) PRIMARY KEY,
    season_type         VARCHAR(10),
    booking_count       INT,
    avg_total_fare      NUMERIC(12, 2),
    pct_vs_non_peak     NUMERIC(6, 2)   -- how much more expensive than non-peak, in %
);

-- KPI 2b: peak vs non-peak in one comparison
CREATE TABLE IF NOT EXISTS kpi_peak (
    season_type    VARCHAR(10) PRIMARY KEY,
    booking_count  INT,
    avg_total_fare NUMERIC(12, 2)
);

-- KPI 4: most popular routes
CREATE TABLE IF NOT EXISTS kpi_route (
    route_rank     INT PRIMARY KEY,
    route          VARCHAR(30),
    source         VARCHAR(10),
    destination    VARCHAR(10),
    booking_count  INT,
    avg_total_fare NUMERIC(12, 2)
);

-- Useful checks (pgAdmin, port 5433):
-- SELECT COUNT(*) FROM flights;
-- SELECT * FROM kpi_airline ORDER BY avg_total_fare DESC;
-- SELECT * FROM kpi_peak;
-- SELECT * FROM kpi_route ORDER BY route_rank;
