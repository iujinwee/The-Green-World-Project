# BC2402 The Green World Project 
# Group 5 

# After importing final_greenworld2022 schema 
use final_greenworld2022;

## DENISE TAY
-- 	1. How many countries are captured in [owid_energy_data]? 
-- 	Note: Be careful! The devil is in the details.
SELECT energy_cty.country FROM (
SELECT DISTINCT
    country, iso_code
FROM
    owid_energy_data 
WHERE
    iso_code NOT LIKE 'OWID%'
        AND iso_code != '' ) as energy_cty INNER JOIN (SELECT entity FROM `continents-according-to-our-world-in-data`) as c  on energy_cty.country = c.entity;
# Total Countries: 217


# Create view for other questions 
CREATE VIEW countrylist AS (SELECT Entity "country" 
								FROM final_greenworld2022.`continents-according-to-our-world-in-data`
								WHERE Code NOT LIKE "OWID%"
                                AND Continent <> Entity);

#################################################         END OF QUESTION 1          ###############################################


## EUGENE WEE
-- 	2. Find the earliest and latest year in [owid_energy_data]. What are the countries
-- 	having a record in <owid_energy_data> every year throughout the entire period (from
-- 	the earliest year to the latest year)?
-- 	Note: The output must provide evidence that the countries have the same number of
-- 	records.

-- 	Getting Min & Max Years
SELECT MIN(year) MinYear, MAX(year) MaxYear, max(year) - min(year) + 1 TotalRecordYear 
	FROM final_greenworld2022.owid_energy_data;
# Min Year: 1900, Max Year: 2021, Total Record Years: 122

-- -- Countries having records every year
SELECT c.country, c.countOfYears
	FROM (SELECT country, count(year) countOfYears
			FROM final_greenworld2022.owid_energy_data
			GROUP BY country) c
	LEFT JOIN final_greenworld2022.countrylist l  # Check for country
    ON c.country = l.country
	WHERE c.countOfYears = 122
    AND l.country IS NOT NULL; 


#################################################         END OF QUESTION 2          ###############################################

## SHERMAINE NG
-- 	3. Specific to Singapore, in which year does <fossil_share_energy> stop being the full
-- 	source of energy (i.e., <100)? Accordingly, show the new sources of energy.

-- 	Year which stops being the full source of energy & new sources of energy
select  year, country, low_carbon_share_energy, other_renewables_share_energy, renewables_share_energy, solar_share_energy
	from final_greenworld2022.owid_energy_data 
	where fossil_share_energy<100 and country = 'Singapore';

# Year 1986. 

#################################################         END OF QUESTION 3          ###############################################


## TOMMY WEE
-- 	4. Compute the average <GDP> of each ASEAN country from 2000 to 2021 (inclusive
-- 	of both years). Display the list of countries based on the descending average GDP
-- 	value.

SELECT country, AVG(gdp) AS avg_gdp FROM owid_countryinfo 
WHERE country IN ("Cambodia", "Indonesia", "Brunei", "Myanmar", "Laos", "Malaysia", "Philippines", "Singapore", "Thailand", "Vietnam") 
AND year BETWEEN 2000 AND 2021
AND gdp != 0
GROUP BY country
ORDER BY avg_gdp DESC; 


#################################################         END OF QUESTION 4          ###############################################

## EUGENE WEE 
-- 	5. (Without creating additional tables/collections) For each ASEAN country, from 2000
-- 	to 2021 (inclusive of both years), compute the 3-year moving average of
-- 	<oil_consumption> (e.g., 1st: average oil consumption from 2000 to 2002, 2nd:
-- 	average oil consumption from 2001 to 2003, etc.). Based on the 3-year moving
-- 	averages, identify instances of negative changes (e.g., An instance of negative
-- 	change is detected when 1st 3-yo average = 74.232, 2nd 3-yo average = 70.353).
-- 	Based on the pair of 3-year averages, compute the corresponding 3-year moving
-- 	averages in GDP.

SELECT o.country, o.duration_3YMA, g.GDP_3YMA
	# Identifying negative instances & retrieving pairs
	FROM (SELECT d.country, d.duration_3YMA, d.oilValue_3YMA, d.instance_diff_lag, d.instance_diff_lead
			# Calculation for finding negative instances
			FROM (SELECT m.country, m.duration_3YMA, m.oilValue_3YMA, 
						 # For negative pair instances: Lead to track 1st MA pair, Lag to track 2nd MA pair
						 m.oilValue_3YMA - LAG(m.oilValue_3YMA) OVER(PARTITION BY country ORDER BY duration_3YMA) "instance_diff_lag",
                         m.oilValue_3YMA - LEAD(m.oilValue_3YMA) OVER(PARTITION BY country ORDER BY duration_3YMA) "instance_diff_lead"
					 # Calculation of 3Y-MA for Oil Consumption 
					 FROM (SELECT ma.country, concat(year-2, " - ", year) "duration_3YMA", 
								  ma.oilValue_3YMA "oilValue_3YMA"
							  FROM (SELECT year, country, 
										   AVG(c.oil_consumption) OVER (ORDER BY country, year
																		ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "oilValue_3YMA",
																		count(*) OVER (PARTITION BY country ORDER BY year) AS "oilIndex_3YMA"
										FROM final_greenworld2022.owid_energy_data c
										WHERE year BETWEEN 2000 AND 2021
										ORDER BY country, year) ma
							  WHERE ma.oilIndex_3YMA-2 > 0
							  # List of ASEAN countries
							  AND ma.country IN ("Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", 
												 "Myanmar", "Philippines", "Singapore", "Thailand", "Vietnam")) m) d
			WHERE d.instance_diff_lag < 0 OR d.instance_diff_lead > 0) o
            
	LEFT JOIN 
		
        # Calculation of 3Y-MA for GDP
		(SELECT ma.country, concat(year-2, " - ", year) "duration_3YMA", 
			   ma.GDP_3YMA "GDP_3YMA"
			FROM (SELECT year, country, 
						AVG(c.gdp) OVER (ORDER BY country, year
										 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS "GDP_3YMA",
														count(*) OVER (PARTITION BY country ORDER BY year) AS "GDPindex_3YMA"
						FROM final_greenworld2022.owid_energy_data c
						WHERE year BETWEEN 2000 AND 2021
						ORDER BY country, year) ma
			WHERE ma.GDPindex_3YMA-2 > 0) g
        
	ON o.country=g.country AND o.duration_3YMA=g.duration_3YMA
    where gdp_3yma is not null;

                    
#################################################         END OF QUESTION 5          ###############################################


## SHERMAINE NG
-- 	6. For each <energy_products> and <sub_products>, display the overall average of
-- 	<value_ktoe> from [importsofenergyproducts] and [exportsofenergyproducts].

select A.energy_products, A.sub_products, AVG(A.value_ktoe) AS import_overallavg, AVG(B.value_ktoe) AS export_overallavg
from final_greenworld2022.importsofenergyproducts A left join final_greenworld2022.exportsofenergyproducts B 
on A.energy_products = B.energy_products and A.sub_products = B.sub_products
where A.value_ktoe is not null and B.value_ktoe is not null
group by energy_products, sub_products; 


#################################################         END OF QUESTION 6          ###############################################

## DENISE TAY
-- 	7. For each combination of <energy_products> and <sub_products>, find the yearly
-- 	difference in <value_ktoe> from [importsofenergyproducts] and
-- 	[exportsofenergyproducts]. Identify those years where more than 4 instances of
-- 	export value > import value can be detected.

## For each combination of <energy_products> and <sub_products>, 
## find the yearly difference in <value_ktoe> from [importsofenergyproducts] and [exportsofenergyproducts]. 
SELECT 
    e.year,
    e.energy_products,
    e.sub_products,
    e.value_ktoe - i.value_ktoe AS yearly_diff
FROM
    exportsofenergyproducts e
        INNER JOIN
    importsofenergyproducts i ON e.sub_products = i.sub_products
        AND e.energy_products = i.energy_products
        AND e.year = i.year
ORDER BY year, energy_products, sub_products; 

## Identify those years where more than 4 instances of export value > import value can be detected.	
SELECT DISTINCT year FROM (
SELECT 
        e.year,
        COUNT(e.year) OVER (PARTITION BY e.year) AS count_year,
            e.energy_products,
            e.sub_products,
            e.value_ktoe - i.value_ktoe AS yearly_diff
    FROM
        exportsofenergyproducts e
    INNER JOIN importsofenergyproducts i ON e.sub_products = i.sub_products
        AND e.energy_products = i.energy_products
        AND e.year = i.year WHERE e.value_ktoe - i.value_ktoe > 0 ) AS TEMP WHERE count_year > 4 ; 
## 2014. 


#################################################         END OF QUESTION 7          ###############################################

## TOMMY WEE 
-- 	8. In [householdelectricityconsumption], for each <region>, excluding “overall”, generate
-- 	the yearly average <kwh_per_acc>.

select region, year, avg(kwh_per_acc)
	from householdelectricityconsumption
    where Region <> "Overall"
	and dwelling_type = "Overall"
    and month = "Annual"
    and Region = Description  # Average of all regions 
    group by year, region
    ORDER BY Region, year;

#################################################         END OF QUESTION 8          ###############################################

## SHERMAINE NG 
-- 	9. Who are the energy-saving stars? Compute the yearly average of <kwh_per_acc> in
-- 	each region, excluding “overall”. Generate the moving 2-year average difference (i.e.,
-- 	year 1 average kwh_per_acc for the central region = 1223, year 2 = 1000, the
-- 	moving 2-year average difference = -223). Display the top 3 regions with the most
-- 	instances of negative 2-year averages.

#Find the yearly avg(kwh_per_acc) in each region
select year, region, avg(kwh_per_acc) as yearlyavg
from final_greenworld2022.householdelectricityconsumption
where Region != 'Overall'
and dwelling_type = 'Overall'
and month = 'Annual'
and region = description
group by Region, year
order by region;

#Find negative 2 year moving average 
select region, moving2Yduration, moving2Yavg from 
(select year, CONCAT(year-1, "-", year) as moving2Yduration,
region, avg(kwh_per_acc) as yearlyavg, lag(avg(kwh_per_acc)) over (partition by region order by year) as prevyearlyavg,
avg(kwh_per_acc) - lag(avg(kwh_per_acc)) over (partition by region order by year) as moving2Yavg
from final_greenworld2022.householdelectricityconsumption 
where Region != 'Overall'
and dwelling_type = 'Overall'
and month = 'Annual'
and region = description
group by Region, year
order by region) as a
where moving2Yavg<0;

#Find the number of instances of negative 2 year moving average
select region, count(moving2Yavg) as NoOfInstances
from (select year, region, moving2Yavg from 
(select year, CONCAT(year-1, "-", year) as moving2Yduration,
region, avg(kwh_per_acc) as yearlyavg, lag(avg(kwh_per_acc)) over (partition by region order by year) as prevyearlyavg,
avg(kwh_per_acc) - lag(avg(kwh_per_acc)) over (partition by region order by year) as moving2Yavg
from final_greenworld2022.householdelectricityconsumption 
where Region != 'Overall'
and dwelling_type = 'Overall'
and month = 'Annual'
and region = description
group by Region, year
order by region) as a
where moving2Yavg<0) as b
group by region order by NoOfInstances desc;


#################################################         END OF QUESTION 9          ###############################################

## EUGENE WEE
--  10. Are there any seasonal (quarterly) effects on energy consumption? Visualizations are
--   	typically required to eyeball the effects. For each region, in each year, compute the
--   	quarterly average in <kwh_per_acc>. Exclude “Overall” in <region>.
-- 	    Note: 1st quarter = January, February, and March, 2nd quarter = April, May, and June,
-- 	    and so on.

# Obtain quarterly average kwh for each region in each year (VALUES ONLY) 
SELECT year, Region, QUARTER(STR_TO_DATE(month, '%m/%d/%Y')) quarter, AVG(kwh_per_acc) "avg_kwh_per_acc"
			FROM final_greenworld2022.householdelectricityconsumption
            # Exclude "Overall" in Region, extract monthly data for overall dwelling type 
            # and Desciption = Region (Overall kwh for that region) 
			WHERE Region <> "Overall" AND month <> "Annual"		
            AND dwelling_type = "Overall" AND Description = Region
            GROUP BY year, Region, quarter	
            ORDER BY year, Region;
	
# Creating view for visualisation (FOR VIEWS, NEED TO CREATE VIEW & USE VARIABLES)
CREATE VIEW quart_avg_kwh AS 
	SELECT year, Region, QUARTER(STR_TO_DATE(month, '%m/%d/%Y')) quarter, AVG(kwh_per_acc) "avg_kwh_per_acc"
		FROM final_greenworld2022.householdelectricityconsumption
		# Exclude "Overall" in Region, extract monthly data for overall dwelling type 
		# and Desciption = Region (Overall kwh for that region) 
		WHERE Region <> "Overall" AND month <> "Annual"		
		AND dwelling_type = "Overall" AND Description = Region
		GROUP BY year, Region, quarter
		ORDER BY year, Region;
 
SET @min_kwh = (SELECT min(avg_kwh_per_acc) FROM quart_avg_kwh);
SET @max_kwh = (SELECT max(avg_kwh_per_acc) FROM quart_avg_kwh);
SELECT *, REPEAT("*", 100*(avg_kwh_per_acc-@min_kwh)/(@max_kwh - @min_kwh)) "Scaled_avg_kwh"
	FROM quart_avg_kwh;
 

#################################################         END OF QUESTION 10          ###############################################


## DENISE TAY
-- 11. Consider [householdtowngasconsumption]. Are there any seasonal (quarterly) effects
--     on town gas consumption? For each <sub_housing_type>, in each year, compute the
-- 	   quarterly average in <avg_mthly_hh_tg_consp_kwh>. Exclude “Overall” in <
-- 	   sub_housing_type>.

SELECT sub_housing_type, year,
CASE
WHEN month BETWEEN 1 and 3 THEN 1
WHEN month BETWEEN 4 and 6 THEN 2
WHEN month BETWEEN 7 and 9 THEN 3
WHEN month BETWEEN 10 and 12 THEN 4 END AS quarter, AVG(avg_mthly_hh_tg_consp_kwh) AS avg_qtrly_hh_tg_consp_kwh FROM householdtowngasconsumption WHERE sub_housing_type NOT IN ("Overall", "Public Housing", "Private Housing") AND month NOT LIKE 'na%' AND month NOT LIKE '%Region%' GROUP BY sub_housing_type, year, CASE
WHEN month BETWEEN 1 and 3 THEN 1
WHEN month BETWEEN 4 and 6 THEN 2
WHEN month BETWEEN 7 and 9 THEN 3
WHEN month BETWEEN 10 and 12 THEN 4 END ORDER BY sub_housing_type, year, quarter; 


#################################################         END OF QUESTION 11          ###############################################



#####################################################################################################################################
###############################################           OPEN-ENDED QUERY           ################################################
#####################################################################################################################################


## SHERMAINE NG 
-- 12. *Open-ended question* How has Singapore been performing in terms of energy
-- consumption? Find a comparable reference(s) to illustrate changes in energy per
-- capita, energy per GDP, and various types of energy (e.g., solar, gas, and oil) over
-- the years. 

#Find Singapore's GDP per capita across the years from 1965 to 2018
select year, country, population, gdp, gdp/population as gdppercapita
 from final_greenworld2022.owid_countryinfo
where  country = 'Singapore' and year >= 1965 and year <= 2018;

#Find comparable countries based on GDP per capita and continent
select a.country, b.Continent, avg(gdppercapita) from (select country, gdp/population as gdppercapita from owid_countryinfo
where year >= 1965 and year <= 2018) as a left join `continents-according-to-our-world-in-data` as b
on a.country = b.entity where continent = 'Asia' group by country, Continent order by avg(gdppercapita);

#Find energy_per_gdp and energy_per_capita for Singapore, Hong Kong
select year, country, energy_per_gdp, energy_per_capita
from owid_countryenergy 
where (country = 'Singapore' or country = 'Hong Kong'or country = 'Japan')
and year >= 1965 and year <= 2018
order by country;

#3 big categories
select year, country, renewables_share_energy, nuclear_share_energy, fossil_share_energy from owid_energy_data 
where (country = 'Singapore' or country = 'Hong Kong' or country = 'Japan') and year >= 1965 and year <= 2020;
select year, country, renewables_consumption, fossil_fuel_consumption from owid_energy_data 
where (country = 'Singapore' or country = 'Hong Kong' or country = 'Japan') and year >= 1965 and year <= 2020;

#low carbon 
select year, country, renewables_consumption, fossil_fuel_consumption, low_carbon_consumption from owid_energy_data 
where (country = 'Singapore' or country = 'Hong Kong' or country = 'Japan') and year >= 1965 and year <= 2020;

#renewable category 
select year, country, other_renewable_consumption, biofuel_consumption, solar_consumption, wind_consumption, hydro_consumption from final_greenworld2022.owid_energy_data 
where (country = 'Singapore' or country = 'Hong Kong' or country = 'Japan') and year >= 1965 and year <= 2020;

#fossil category 
select year, country, oil_consumption, coal_consumption, gas_consumption from final_greenworld2022.owid_energy_data 
where (country = 'Singapore' or country = 'Hong Kong' or country = 'Japan') and year >= 1965 and year <= 2020;


################################################         END OF Q12          ##################################################

## EUGENE WEE
-- 13. Can renewable energy adequately power continued economic growth? 

## Sustaining Energy Demand 
# Figure: World Population, Energy & Electricity Demand since 2000
SELECT year, population, electricity_demand, primary_energy_consumption
	FROM final_greenworld2022.owid_energy_data
    WHERE country = "World" 
    AND year >= 2000;


# Figure: World Energy & Electricity Consumption by Type (‘00 vs ‘20)
SELECT *, 100*r.Fossil/r.total "pct_fossil", 100*r.Renewables/r.total "pct_renewables", 100*r.Nuclear/r.total "pct_nuclear"
	FROM (SELECT year, primary_energy_consumption "Total", fossil_fuel_consumption "Fossil", 
			renewables_consumption+other_renewable_consumption "Renewables", nuclear_consumption "Nuclear"
			FROM final_greenworld2022.owid_energy_data
			WHERE country = "World"
			AND year IN (2000, 2020)) r;
            
            
# Figure: Percentage Change in Energy Consumption by Type Since 2000 
SELECT year, solar_cons_change_pct, wind_cons_change_pct, biofuel_cons_change_pct, 
		hydro_cons_change_pct, nuclear_cons_change_pct, other_renewables_cons_change_pct
	FROM final_greenworld2022.owid_energy_data
    WHERE country = "World"
    AND year >= 2000;


##########################################################

## Adequate Energy Generation 
# Figure: Breakdown of energy by type (2020 since 2021 no data)
SELECT year, fossil_share_energy, solar_share_energy, wind_share_energy, biofuel_share_energy, 
		hydro_share_energy, nuclear_share_energy, other_renewables_share_energy
	FROM final_greenworld2022.owid_energy_data
    WHERE country = "World"
    AND year = 2020;
  
# Figure: Breakdown of electricity by type (2021)
SELECT year, fossil_share_elec, solar_share_elec, wind_share_elec, biofuel_share_elec, 
		hydro_share_elec, nuclear_share_elec, other_renewables_share_elec_exc_biofuel
	FROM final_greenworld2022.owid_energy_data
    WHERE country = "World"
    AND year = 2021;
    

# Figure: World Energy Generation (per capita) 
SELECT year, per_capita_electricity, coal_elec_per_capita+gas_elec_per_capita+oil_elec_per_capita "fossil_elect_per_capita",
		solar_elec_per_capita+wind_elec_per_capita+biofuel_elec_per_capita+
        hydro_elec_per_capita+other_renewables_elec_per_capita "renewables_elec_per_capita", nuclear_elec_per_capita
	FROM final_greenworld2022.owid_energy_data
    WHERE country = "World"
    AND year >= 2000;

# Figure: World Electricity Generation by Type (per capita) since 2000
SELECT year, electricity_demand, solar_electricity, wind_electricity, hydro_electricity, biofuel_electricity, other_renewable_electricity
	FROM final_greenworld2022.owid_energy_data 
	where year >= 2000
    and country = "World";


# Figure: Electricity Source by Continent (‘00 vs ‘21)
SELECT r.year, r.Continent, r.fossil, r.renewables, r.nuclear, r.renewables/r.elec "pct_elect_by_renew", r.nuclear/r.elec "pct_elect_by_nuclear"
	FROM (SELECT o.year, c.Continent, AVG(per_capita_electricity) "elec", 
			AVG(coal_elec_per_capita+gas_elec_per_capita+oil_elec_per_capita) "fossil",
			AVG(solar_elec_per_capita+wind_elec_per_capita+biofuel_elec_per_capita+
			hydro_elec_per_capita+other_renewables_elec_per_capita_exc_biofuel) "renewables", 
			AVG(nuclear_elec_per_capita) "nuclear"
		FROM final_greenworld2022.owid_energy_data o
		LEFT JOIN final_greenworld2022.`continents-according-to-our-world-in-data` c
		ON o.country = c.Entity
		WHERE c.Continent IS NOT NULL
		AND o.year IN (2000, 2021)
		GROUP BY c.Continent, o.year
		ORDER BY c.Continent) r
	WHERE fossil <> 0;

## Economic Viability of Renewable Energy
# Figure: Extracting countries with highest consumption of renewables vs electricity demand
SELECT year, r.country, renewables_consumption "Renewables Consumption", gdp/population "gdp_per_capita"
	FROM final_greenworld2022.owid_energy_data o
    INNER JOIN (SELECT DISTINCT country
						FROM final_greenworld2022.owid_energy_data
						WHERE country in (select country from countrylist)
						GROUP by country
						ORDER BY AVG(renewables_consumption/electricity_demand) DESC
						LIMIT 5) r
	ON o.country = r.country
    WHERE year >= 2000
    and gdp is not null;
    
          
### COMPARING GDP AND RENEWABLES CONSUMPTION
# Breakdown of gdp per capita and renewables share elec by continents
select continent, avg(renewables_share_elec), avg(gdp/population) 
	FROM final_greenworld2022.owid_energy_data o
	left join final_greenworld2022.`continents-according-to-our-world-in-data` c
	on o.country = c.entity
    where gdp is not null and Continent is not null
    group by Continent;


-- Figure: GDP per capita of top renewables (EUROPE) 
	(SELECT r.country, avg(renewables_share_elec) "Renewables Share Elec", avg(gdp/population) "GDP per capita"
		FROM final_greenworld2022.owid_energy_data o
		INNER JOIN (SELECT DISTINCT country
							FROM final_greenworld2022.owid_energy_data o
                            left join final_greenworld2022.`continents-according-to-our-world-in-data` c
                            on o.country = c.entity
                            where continent = "Europe"
                            and country in (select country from countrylist)
							GROUP by country
							ORDER BY AVG(renewables_consumption/electricity_demand) DESC
							LIMIT 5) r
		ON o.country = r.country
		WHERE year >= 2000
		and gdp is not null
		group by country
		order by avg(gdp/population) desc)
union 
	(select c.Continent, avg(renewables_share_elec) "Renewables Share Elec", avg(gdp/population) "GDP per capita"
		FROM final_greenworld2022.owid_energy_data o
		left join final_greenworld2022.`continents-according-to-our-world-in-data` c
		on c.Entity = o.country
		where Continent = "Europe"
		and o.year >= 2000
		group by c.Continent);
	


-- Figure: GDP per capita of top renewables (North AMERICA) 
	(SELECT r.country, avg(renewables_share_elec) "Renewables Share Elec", avg(gdp/population) "GDP per capita"
		FROM final_greenworld2022.owid_energy_data o
		INNER JOIN (SELECT DISTINCT country
							FROM final_greenworld2022.owid_energy_data o
                            left join final_greenworld2022.`continents-according-to-our-world-in-data` c
                            on o.country = c.entity
                            where continent = "North America"
                            and country in (select country from countrylist)
							GROUP by country
							ORDER BY AVG(renewables_consumption/electricity_demand) DESC
							LIMIT 3) r
		ON o.country = r.country
		WHERE year >= 2000
		and gdp is not null
		group by country
		order by avg(gdp/population) desc)
union 
	(select c.Continent, avg(renewables_share_elec) "Renewables Share Elec", avg(gdp/population) "GDP per capita"
		FROM final_greenworld2022.owid_energy_data o
		left join final_greenworld2022.`continents-according-to-our-world-in-data` c
		on c.Entity = o.country
		where Continent = "North America"
		and o.year >= 2000
		group by c.Continent);


-- Figure: GDP per capita of top renewables (SOUTH AMERICA) 
	(SELECT r.country, avg(renewables_share_elec) "Renewables Share Elec", avg(gdp/population) "GDP per capita"
		FROM final_greenworld2022.owid_energy_data o
		INNER JOIN (SELECT DISTINCT country
							FROM final_greenworld2022.owid_energy_data o
                            left join final_greenworld2022.`continents-according-to-our-world-in-data` c
                            on o.country = c.entity
                            where continent = "South America"
                            and country in (select country from countrylist)
							GROUP by country
							ORDER BY AVG(renewables_consumption/electricity_demand) DESC
							LIMIT 5) r
		ON o.country = r.country
		WHERE year >= 2000
		and gdp is not null
		group by country
		order by avg(gdp/population) desc)
union 
	(select c.Continent, avg(renewables_share_elec) "Renewables Share Elec", avg(gdp/population) "GDP per capita"
		FROM final_greenworld2022.owid_energy_data o
		left join final_greenworld2022.`continents-according-to-our-world-in-data` c
		on c.Entity = o.country
		where Continent = "South America"
		and o.year >= 2000
		group by c.Continent);



## By Energy Types (LEADING COUNTRY)

# Wind Share vs GDP per capita
select o.year, avg(o.wind_share_elec) "Wind Share Elec", avg(o.gdp/o.population) "Average GDP per capita"
	from (select country 
				from final_greenworld2022.owid_energy_data
				where year >= 2000 and gdp is not null
				group by country 
				order by avg(wind_share_elec) desc
				limit 5) r
    left join final_greenworld2022.owid_energy_data o
	on o.country = r.country
    where year >= 2000
    and gdp is not null
    group by year;

# Solar Share vs GDP per capita
select o.year, avg(o.solar_share_elec) "Solar Share Elec", avg(o.gdp/o.population) "Average GDP per capita"
	from (select country
				from final_greenworld2022.owid_energy_data
				where year >= 2000 and gdp is not null
                and solar_consumption is not null
				group by country 
				order by avg(solar_share_elec) desc
				limit 5) r
    left join final_greenworld2022.owid_energy_data o
	on o.country = r.country
    where year >= 2000
    and gdp is not null
    group by year;

# Hydro Consumption vs GDP per capita
select o.year, avg(o.hydro_share_elec) "Hydro Share Elec", avg(o.gdp/o.population) "Average GDP per capita"
	from (select country
				from final_greenworld2022.owid_energy_data
				where year >= 2000 and gdp is not null
                and country in (select country from countrylist)
                and hydro_consumption is not null
				group by country 
				order by avg(hydro_share_elec) desc
				limit 5) r
    left join final_greenworld2022.owid_energy_data o
	on o.country = r.country
    where year >= 2000
    and gdp is not null
    group by year;


# Nuclear Consumption vs GDP per capita
select o.year, avg(o.nuclear_consumption) "Average Nuclear Consumption", avg(o.gdp/o.population) "Average GDP per capita"
	from (select country
				from final_greenworld2022.owid_energy_data
				where year >= 2000 and gdp is not null
                and country in (select country from countrylist)
                and nuclear_consumption is not null
				group by country 
				order by avg(nuclear_share_elec) desc
				limit 5) r
    left join final_greenworld2022.owid_energy_data o
	on o.country = r.country
    where year >= 2000
    and gdp is not null
    group by year;


################################################         END OF Q13          ##################################################

## DENISE TAY
-- 14. NUCLEAR ENERGY

##2010-2020 Singapore's Energy product Trade
SELECT 
    i.year, i.total_import_value_ktoe, e.total_export_value_ktoe
FROM
    (SELECT 
        year, SUM(value_ktoe) total_import_value_ktoe
    FROM
        importsofenergyproducts
    GROUP BY year) AS i
        INNER JOIN
    (SELECT 
        year, SUM(value_ktoe) total_export_value_ktoe
    FROM
        exportsofenergyproducts
    GROUP BY year) AS e ON i.year = e.year
WHERE
    i.year >= '2010'; 

############################################

##2010-2020 Singapore's Balance of Energy Product Trade 
SELECT 
    i.year,
    (e.total_export_value_ktoe - i.total_import_value_ktoe) AS balance_value_ktoe
FROM
    (SELECT 
        year, SUM(value_ktoe) total_import_value_ktoe
    FROM
        importsofenergyproducts
    GROUP BY year) AS i
        INNER JOIN
    (SELECT 
        year, SUM(value_ktoe) total_export_value_ktoe
    FROM
        exportsofenergyproducts
    GROUP BY year) AS e ON i.year = e.year
WHERE
    i.year >= '2010'; 
    
############################################

##2010-2020 Low Carbon Electricity By Type : low_carbon energy incl wind, solar, hydro or nuclear power (renewables + nuclear = low carbon) 
SELECT 
    year,
    wind_electricity,
    hydro_electricity,
    solar_electricity,
    nuclear_electricity,
    other_renewable_electricity
FROM
    owid_energy_data
WHERE
    country = 'World' AND year >= '2000';
## shows that nuclear is the second largest contributor of low carbon electrcity in the world

############################################ 

## 2000 & 2020 Singapore electricity mix 
SELECT 
    year,
    solar_share_elec,
    other_renewables_share_elec,
    fossil_share_elec,
    renewables_share_elec
FROM
    owid_energy_data
WHERE
    country = 'Singapore'
        AND year IN (2020 , 2000);
        
############################################

## Singapore efficiency by type (2020) 
### Assumption: Singapore's only source of renewable energy is solar energy (Hence renewable energy in singapore == solar) 
SELECT 
    year,
    solar_electricity / solar_consumption AS solar_efficiency,
    other_renewable_electricity / other_renewable_consumption AS other_renewable_efficiency,
    fossil_electricity / fossil_fuel_consumption AS fossil_efficiency
FROM
    owid_energy_data
WHERE
    country = 'Singapore' AND year >= 2000;

############################################
        
## 2020 Global Energy Mix 
SELECT 
    year,
    fossil_share_energy, 
    renewables_share_energy,
    other_renewables_share_energy,
    nuclear_share_energy
FROM
    owid_energy_data
WHERE
    country = 'World' AND year = '2020';

############################################

## 2020 Global Electricity Mix 
SELECT 
    year,
    fossil_share_elec,
    renewables_share_elec,
    other_renewables_share_elec,
    nuclear_share_elec
FROM
    owid_energy_data
WHERE
    country = 'World' AND year = '2020';

############################################

##Find top 3 countries with highest nuclear production 
SELECT 
    AVG(nuclear_electricity), country
FROM
    owid_nuclear
GROUP BY country
ORDER BY AVG(nuclear_electricity) DESC; 

## Top 3 countries of electricity generation are: United States, Russia, France

############################################

## Top 3 Nuclear Generating Countries 
### Nuclear share of electricity vs carbon intensity of electricity (Russia, France, United States) &
### Nuclear share of electricity vs GDP per capita (Russia, France, United States)
SELECT 
    year,
    country,
    carbon_intensity_elec,
    gdp / population AS gdp_per_capita,
    nuclear_share_elec
FROM
    owid_energy_data
WHERE
    country IN ('United States' , 'Russia', 'France')
        AND year >= 2000; 

############################################

## Efficiency by type for top 3 nuclear generating countries (2020) 
SELECT 
    year,
    country,
    nuclear_electricity / nuclear_consumption AS nuclear_efficiency,
    fossil_electricity / fossil_fuel_consumption AS fossil_efficiency,
    renewables_electricity / renewables_consumption AS renewable_efficiency
FROM
    owid_energy_data
WHERE
    country IN ('United States' , 'Russia', 'France')
        AND year = '2020'; 
        



################################################         END OF Q14          ##################################################

-- 15. Debunking Skepticism behind Climate Change (TOMMY WEE) 

SELECT year, total_fossil_fuel_co2, total_co2, total_ghg, 
(total_fossil_fuel_co2/total_co2) AS fossil_fuel_co2_pct, (total_fossil_fuel_co2/total_ghg) AS fossil_fuel_ghg_pct, (total_co2/total_ghg) AS co2_ghg_pct FROM
	(SELECT year, SUM(coal_co2) as total_coal_co2, SUM(gas_co2) AS total_gas_co2, SUM(oil_co2) AS total_oil_co2, SUM(coal_co2+gas_co2+oil_co2) AS total_fossil_fuel_co2,
	SUM(co2) AS total_co2, SUM(total_ghg) AS total_ghg
	FROM `owid-co2-data` WHERE year > 1990 GROUP BY year ORDER BY year) AS a;
    
SELECT AVG(fossil_fuel_co2_pct) AS avg_fossil_fuel_co2_pct, AVG(co2_ghg_pct) AS avg_co2_ghg_pct FROM
	(SELECT year, total_fossil_fuel_co2, total_co2, total_ghg, 
	(total_fossil_fuel_co2/total_co2) AS fossil_fuel_co2_pct, (total_fossil_fuel_co2/total_ghg) AS fossil_fuel_ghg_pct, (total_co2/total_ghg) AS co2_ghg_pct FROM
		(SELECT year, SUM(coal_co2) as total_coal_co2, SUM(gas_co2) AS total_gas_co2, SUM(oil_co2) AS total_oil_co2, SUM(coal_co2+gas_co2+oil_co2) AS total_fossil_fuel_co2,
		SUM(co2) AS total_co2, SUM(total_ghg) AS total_ghg
		FROM `owid-co2-data` WHERE year > 1990 GROUP BY year ORDER BY year) AS a) AS b;

SELECT a.year, avg_yearly_temp, total_ghg FROM
    (SELECT YEAR(recordedDate) AS year, AVG(LandAverageTemperature) AS avg_yearly_temp FROM globaltemperatures 
	WHERE LandAverageTemperature != 0 AND YEAR(recordedDate) > 1990 GROUP BY year) AS a
INNER JOIN
	(SELECT year, SUM(total_ghg) AS total_ghg FROM `owid-co2-data` WHERE year > 1990 GROUP BY year ORDER BY year) AS b
ON a.year = b.year;

# Figure: Land Temp + SeaIce + SeaLevels 
select i.year, avg_sealevel_chg_in_mm,  avg_seaice_extent, avg_landtemp
	from (select year, avg(extent) avg_seaice_extent from external_seaice group by year) i
	left join final_greenworld2022.external_sealevelchg s
    on s.year=i.year
    left join (select left(recordedDate,4) "year", avg(LandAverageTemperature) avg_landtemp from globaltemperatures group by left(recordedDate,4)) t
    on i.year = t.year;
    


# Societal Detriments (EUGENE WEE) 

-- Figure: Impact of Rising Sea levels (Population at risk) For 2010
select c.Continent, avg(f.`2010`) "below_5m", 100-avg(f.`2010`) "above_5m"
	from final_greenworld2022.external_population_under_5_metres f
    left join final_greenworld2022.`continents-according-to-our-world-in-data` c
    on f.Country = c.Entity
    where f.Country in (SELECT Country from countrylist)
    group by Continent
    order by avg(f.`2010`) desc ; 
    

# Figure: Climate Change related Disaster Frequency (1980 - 2021)
-- Time-series globally, by type (total disasters) per year VS rising sealevels 
select o.year, "World",  f.type, sum(f.value) "disaster_count"
	from final_greenworld2022.external_cc_disasterfreq f
	left join final_greenworld2022.owid_countryinfo o
    on f.year = o.year and f.country = o.country
    where o.year is not null
    group by year, type;
   
   
-- Economic & Societal Impact

# Figure: Total Economic Damage & Deaths by Types (2000 - 2019)
select e.entity, avg(e.`Total economic damage (EMDAT (2020))`) "total_eco_damages", avg(d.`Total deaths (EMDAT (2020))`) "total_deaths"
	from final_greenworld2022.external_economic_damage_from_natural_disasters e
    left join final_greenworld2022.external_deaths_from_natural_disasters_by_type d
    on e.Entity = d.Entity
    and e.year=d.year
    where e.year >= 2000
    group by e.entity
    order by avg(e.`Total economic damage (EMDAT (2020))`) desc;
    

# Figure: Percentage of Internal Displacements due to disasters vs GDP per capita by Continents 
select Continent, avg(displacements), avg(population), 100*avg(displacements/population) "% Displacement", 
		avg(gdp/population) "GDP per Capita"
	from external_disaster_displacement d
    left join final_greenworld2022.`continents-according-to-our-world-in-data` c
    on c.Entity = d.country
    left join final_greenworld2022.owid_countryinfo o
    on o.country=d.Country
	where d.country in (select country from countrylist)
    and `Hazard Type` = "Flood"
    group by Continent 
    order by avg(displacements) desc;
    
    
-- Continent level
select Continent, avg(displacements), avg(population), 100*avg(displacements/population) pct_displacement,
		avg(m.`2010`) 'pop_under_5', avg(gdp/population)
	from external_disaster_displacement d
	left join external_population_under_5_metres m
    on d.country = m.Country
    left join final_greenworld2022.`continents-according-to-our-world-in-data` c
    on c.Entity = d.country
    left join final_greenworld2022.owid_countryinfo o
    on o.country=d.Country
	where d.country in (select country from countrylist)
    and `Hazard Type` = "Flood"
    group by Continent 
    order by avg(displacements) desc;

# Time-series on economic damage & losses     
-- Figure: Economic Losses from disasters (pct_of_totalgdp)
select entity, avg(economic_loss_from_disasters_USD) total_economic_loss_from_disaster_USD, avg(c.population) "avg_population", avg(c.gdp) avg_gdp,
		avg(economic_loss_from_disasters_USD/c.gdp)*100 "pct_economic_loss"
	from external_direct_disaster_economic_loss e
    left join (select * from owid_countryinfo) c
    on e.entity = c.country 
    where population >= (select avg(population) from owid_countryinfo)
	group by entity
    order by avg(economic_loss_from_disasters_USD/c.gdp) desc;
    

################################################         END OF Q15          ##################################################

##########################################         END OF MYSQL QUERY         #################################################
