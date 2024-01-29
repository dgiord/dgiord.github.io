--First of all, we create the necessary tables in which we will import the data from the dataset

CREATE TABLE IF NOT EXISTS renewable_share(
    "Entity" text COLLATE pg_catalog."default",
    "Code" text COLLATE pg_catalog."default",
    "Year" bigint,
    "Renewables (% equivalent primary energy)" double precision);
CREATE TABLE IF NOT EXISTS modern_renewable_energy_consumption(
    "Entity" text COLLATE pg_catalog."default",
    "Code" text COLLATE pg_catalog."default",
    "Year" bigint,
    "Geo Biomass Other - TWh" double precision,
    "Solar Generation - TWh" double precision,
    "Wind Generation - TWh" double precision,
    "Hydro Generation - TWh" double precision);
CREATE TABLE IF NOT EXISTS modern_renewable_production(
    "Entity" text COLLATE pg_catalog."default",
    "Code" text COLLATE pg_catalog."default",
    "Year" bigint,
    "Electricity from wind (TWh)" double precision,
    "Electricity from hydro (TWh)" double precision,
    "Electricity from solar (TWh)" double precision
    "Other renewables including bioenergy (TWh)" double precision);
CREATE TABLE IF NOT EXISTS hydro_share_energy(
    "Entity" text COLLATE pg_catalog."default",
    "Code" text COLLATE pg_catalog."default",
    "Year" bigint,
    "Hydro (% equivalent primary energy)" double precision);
CREATE TABLE IF NOT EXISTS wind_share_energy(
    "Entity" text COLLATE pg_catalog."default",
    "Code" text COLLATE pg_catalog."default",
    "Year" bigint,
    "Wind (% equivalent primary energy)" double precision);
CREATE TABLE IF NOT EXISTS solar_share_energy(
    "Entity" text COLLATE pg_catalog."default",
    "Code" text COLLATE pg_catalog."default",
    "Year" bigint,
    "Solar (% equivalent primary energy)" double precision);
-- We then import the data using the built-in pgAdmin 4 "Import/Export" tool

-- After importing the data in the tables, we clean it by deleting the rows which contain aggregate groups of countries 
-- (e.g. North America), as our analysis will only be focused on individual countries.
-- To do so, we use the DELETE function and delete all the rows in which "Code" is null, 
-- as country aggregates won't have a country code.

DELETE FROM hydro_share_energy
WHERE hydro_share_energy."Code" IS null;
DELETE FROM modern_renewable_energy_consumption
WHERE modern_renewable_energy_consumption."Code" IS null;
DELETE FROM modern_renewable_production
WHERE modern_renewable_production."Code" IS null;
DELETE FROM renewable_share
WHERE renewable_share."Code" IS null;
DELETE FROM solar_share_energy
WHERE solar_share_energy."Code" IS null;
DELETE FROM wind_share_energy
WHERE wind_share_energy."Code" IS null;

-- We create a new table "countries" in which we import only the code and entity of each country + World
-- We then set "code" as the primary key for the table, and add it as a foreign key in all other tables
-- to preserve the data in case it's needed further in the analysis

CREATE TABLE countries(
    code text,
    entity text,
    PRIMARY KEY (country_id));
INSERT INTO countries
  SELECT "Code"
  FROM renewable_share
  WHERE renewable_share."Year" = '2000';
UPDATE countries
  SET entity = renewable_share."Entity"
  FROM renewable_share
  WHERE countries.country_id = renewable_share."Code";
-- Having given the condition Year = 2000, USSR was left out, as its data stops in 1984
-- We add it manually using a new INSERT
INSERT INTO countries (code,entity)
  VALUES ('OWID_USS','USSR');

-- We can now set the Code column as foreign key in all the tables
ALTER TABLE hydro_share_energy
  ADD CONSTRAINT "Code" FOREIGN KEY ("Code")
  REFERENCES countries (code);
ALTER TABLE modern_renewable_energy_consumption
  ADD CONSTRAINT "Code" FOREIGN KEY ("Code")
  REFERENCES countries (code);
ALTER TABLE renewable_share
  ADD CONSTRAINT "Code" FOREIGN KEY ("Code")
  REFERENCES countries (code);
ALTER TABLE solar_share_energy
  ADD CONSTRAINT "Code" FOREIGN KEY ("Code")
  REFERENCES countries (code);
ALTER TABLE wind_share_energy
  ADD CONSTRAINT "Code" FOREIGN KEY ("Code")
  REFERENCES countries (code);

-- Lastly, we rename some of the columns in the tables for simpler queries
ALTER TABLE hydro_share_energy 
RENAME COLUMN "Hydro (% equivalent primary energy)" TO hydro_percentage;

ALTER TABLE modern_renewable_energy_consumption
RENAME COLUMN "Geo Biomass Other - TWh" TO geo_twh;
ALTER TABLE modern_renewable_energy_consumption
RENAME COLUMN "Solar Generation - TWh" TO solar_twh;
ALTER TABLE modern_renewable_energy_consumption
RENAME COLUMN "Wind Generation - TWh" to wind_twh;
ALTER TABLE modern_renewable_energy_consumption
RENAME COLUMN "Hydro Generation - TWh" to hydro_twh;

ALTER TABLE modern_renewable_production
RENAME COLUMN "Electricity from hydro (TWh)" TO solar_prod_twh;
ALTER TABLE modern_renewable_production
RENAME COLUMN "Electricity from solar (TWh)" TO solar_prod_twh;
ALTER TABLE modern_renewable_production
RENAME COLUMN "Electricity from wind (TWh)" TO wind_prod_twh;
ALTER TABLE modern_renewable_production
RENAME COLUMN "Other renewables including bioenergy (TWh)" TO other_renewables_twh;

ALTER TABLE renewable_share
RENAME COLUMN "Renewables (% equivalent primary energy)" TO renewables_percentage;

ALTER TABLE solar_share_energy
RENAME COLUMN "Solar (% equivalent primary energy)" TO solar_percentage;

ALTER TABLE wind_share_energy
RENAME COLUMN "Wind (% equivalent primary energy)" TO wind_percentage;

-- ANALYSIS
-- We start our analysis by looking at the 20 countries who have the best percentage
-- in renewables usage in the latest year available for our analysis

SELECT * FROM renewable_share
WHERE "Year" = (SELECT MAX("Year") FROM renewable_share)
ORDER BY renewables_percentage DESC
LIMIT 20;

-- This query aims at discovering the countries with the highest consumption (in TWh) of renewable energy
-- In the modern_renewable_energy_consumption table, we have the total consumption for each renewable energy source
-- We sum them using the COALESCE() function, which will use '0' instead of 'null' in case a column is empty

SELECT "Entity", "Year",
COALESCE(geo_twh ,0) + COALESCE(solar_twh,0) + COALESCE(wind_twh, 0) + COALESCE (hydro_twh,0)
  AS tot_consumption_twh
FROM modern_renewable_energy_consumption
WHERE "Year" = (SELECT MAX("Year") FROM modern_renewable_energy_consumption)
  AND "Entity" <> 'World'
ORDER BY tot_consumption_twh DESC;

-- In the same way, we can find the countries with the highest production (in TWh) of renewable energy
-- However, since the latest year available for this table is 2022 (with data from only 27 countries),
-- we will use the latest year available in the consumption table, in order to have an accurate comparison

SELECT "Entity","Year", 
COALESCE(wind_prod_twh,0) + COALESCE(hydro_prod_twh,0) + COALESCE (solar_prod_twh,0) + COALESCE (other_renewables_twh,0)
  AS tot_renewable_production_twh
FROM modern_renewable_production
WHERE "Year" = (SELECT MAX("Year") FROM modern_renewable_energy_consumption)
  AND "Entity" <> 'World'
ORDER BY tot_renewable_production_twh DESC;

-- We can now compare the results by using the JOIN function to have the two results close to each other.
-- We then create the renewables_surplus column, which is the difference between the total production and total consumption.
-- The countries which have a positive surplus use less renewable energy than what they produce, whereas those with a negative surplus
-- import part of their renewable energy consumption.

SELECT c."Entity",c."Year",
COALESCE(c.geo_twh ,0) + COALESCE(c.solar_twh,0) + COALESCE(c.wind_twh, 0) + COALESCE (c.hydro_twh,0)
  AS tot_consumption,
COALESCE(p.wind_prod_twh,0) + COALESCE(p.hydro_prod_twh,0) + COALESCE (p.solar_prod_twh,0) + COALESCE (p.other_renewables_twh,0)
  AS tot_production,
(COALESCE(p.wind_prod_twh,0) + COALESCE(p.hydro_prod_twh,0) + COALESCE (p.solar_prod_twh,0) + COALESCE (p.other_renewables_twh,0)) -
(COALESCE(c.geo_twh ,0) + COALESCE(c.solar_twh,0) + COALESCE(c.wind_twh, 0) + COALESCE (c.hydro_twh,0))
  AS renewables_surplus
FROM modern_renewable_energy_consumption c
JOIN modern_renewable_production p 
ON c."Entity" = p."Entity" 
  AND c."Code" = p."Code" 
  AND c."Year" = p."Year"
WHERE c."Year"= (SELECT MAX ("Year") from modern_renewable_energy_consumption)
ORDER BY renewables_surplus ASC;

-- Now, we focus on the individual percentages from the tables we have available, and calculate the total
-- percentage for each renewable energy type as tot_percentage, then classify it as 'High' if it's more than 40%,
-- 'Moderate' if it's between 20% and 40%, and "Low" if it's less than 20%, and only take into account the last 3 years
-- available in the dataset.

SELECT w."Entity", w."Year", w.wind_percentage, s.solar_percentage, h.hydro_percentage,
w.wind_percentage + s.solar_percentage + h.hydro_percentage AS tot_percentage,
CASE
  WHEN w.wind_percentage + s.solar_percentage + h.hydro_percentage >= 40 THEN 'High'
  WHEN w.wind_percentage + s.solar_percentage + h.hydro_percentage >= 20 THEN 'Moderate'
  ELSE 'Low'
  END AS percentage_category
FROM wind_share_energy w
JOIN solar_share_energy s 
  ON s."Entity" = w."Entity" 
  AND s."Year" = w."Year"
JOIN hydro_share_energy h
  ON h."Entity" = w."Entity"
  AND h."Year" = w."Year"
WHERE w."Year" > '2019'
ORDER BY tot_percentage DESC;

-- We now use this query to find the variation in the number of countries whose percentage classifies as 'High',
-- 'Moderate', and 'Low' using the COUNT() and CASE () functions. To find relevant results we expand the query to
-- include results from 2015 onwards.

SELECT w."Year",
  COUNT(CASE WHEN w.wind_percentage + s.solar_percentage + h.hydro_percentage >= 40 THEN 1 END) AS high_count,
  COUNT(CASE WHEN w.wind_percentage + s.solar_percentage + h.hydro_percentage >= 20
  AND w.wind_percentage + s.solar_percentage + h.hydro_percentage < 40 THEN 1 END) AS moderate_count,
  COUNT(CASE WHEN w.wind_percentage + s.solar_percentage + h.hydro_percentage < 20 THEN 1 END) AS low_count
FROM wind_share_energy w
JOIN solar_share_energy s
  ON s."Entity" = w."Entity"
  AND s."Year" = w."Year"
JOIN hydro_share_energy h
  ON h."Entity" = w."Entity"
  AND h."Year" = w."Year"
WHERE w."Year" > '2015'
GROUP BY w."Year"
ORDER BY w."Year";

-- Lastly, we will use this query to find the total amount of renewable energy consumed worldwide to see
-- its variation from 1965 to 2021. We use the CAST() function to convert the result of the SUM() function
-- from double precision to decimal, then we round it to the first 2 decimal numbers using the ROUND() function.

SELECT "Entity", "Year",
  ROUND(CAST(SUM(geo_twh + solar_twh + wind_twh + hydro_twh) AS decimal),2) AS renewables_twh
FROM modern_renewable_energy_consumption
WHERE "Entity" = 'World'
GROUP BY "Entity","Year"
ORDER BY "Year";
