/* 
   BC2402 The Green World Project 
   Group 5 
   Query Scripts
*/

// After importing final databases (finalSGYearlyData & finalOWIDdata) 
use final_energy_db


//////////////////////////////////            START OF QUESTION 1 - 11             ////////////////////////////////////


/*  DENISE TAY  */

// 1. How many countries are captured in [owid_energy_data]?
// Note: Be careful! The devil is in the details.

// Extracting countries based on OWID countries list 
db.finalOWIDdata.aggregate([
    // Lookup from OWID countries table
    {$lookup: {
        from: "countriesOWID",
        localField: "country",
        foreignField: "Entity",
        as: "OWIDcountries"
    }},
    
    {$unwind: "$OWIDcountries"},
    
    // Extract countries w/o OWID iso_code & empty arrays 
    {$match: {
        $expr:{
            $and: [
                {$and: [
                    {$ne: ["$OWIDcountries", []]},
                    {$not: {$regexMatch: {
                        input: "$OWIDcountries.Code", regex: /OWID/}}
                    }
                ]}, 
                {$ne: ["$OWIDcountries.Entity", "$OWIDcountries.Continent"]}
            ]
        }  
    }},
    
    // Project Countries
    {$project: {
        "_id":0,
        "country":1,
        "iso_code": "$OWIDcountries.Code"
    }},
    
    // {$out: "finalCountries"}
])

// Count of countries
db.finalCountries.aggregate([
    {$group: {
        "_id":null,
        "countriesCount": {
            $sum: 1
        }
    }},
    {$project:{
        "_id":0,
        "countriesCount": 1
    }}
])

// ANS: 217 Countries


//////////////////////////////////               END OF QUESTION 1               ////////////////////////////////////


/*  EUGENE WEE  */

// 	2. Find the earliest and latest year in [owid_energy_data]. What are the countries
//     having a record in <owid_energy_data> every year throughout the entire period (from
//     the earliest year to the latest year)?
//     Note: The output must provide evidence that the countries have the same number of
//     records.

// Getting Min & Max Years (Min: 1900, Max: 2021)
db.finalOWIDdata.aggregate([
    {$unwind: "$yearlyData"},
    {$group:{
        "_id": null,
        "minYear": {$max: "$yearlyData.year"},
        "maxYear": {$min: "$yearlyData.year"}
    }},
    {$project: {
        "_id": 0,
        "minYear":1, 
        "maxYear":1
    }}
])

// Countries having records every year (44) 
db.finalOWIDdata.aggregate([
    
    // Records every year 
    {$match: {
        $expr: {$eq: [{$size: "$yearlyData"}, 2021-1900+1]}
    }},
    
    // Lookup & Match Countries from Q1
    {$lookup: {
        from: "finalCountries",
        localField: "country",
        foreignField: "country"
        as: "countries"
    }}, 
    
    {$match: {
        $expr: {$ne: ["$countries", []]} // Match non-empty arrays
    }},
    
    {$project: {
        "_id": 0,
        "country": "$country",
        "yearlyData": {
            $sortArray: {
                input: "$yearlyData",
                sortBy: {"year": 1}
            }
        }
    }}
])


//////////////////////////////////               END OF QUESTION 2                ////////////////////////////////////

/*  SHERMAINE NG  */

// 3. Specific to Singapore, in which year does <fossil_share_energy> stop being the full
// source of energy (i.e., <100)? Accordingly, show the new sources of energy.

// Year which stops being the full source of energy

db.finalOWIDdata.aggregate([
    
    {$match: {"country": "Singapore"}},
    {$unwind: "$yearlyData"},
    {$sort: {"yearlyData.year": 1}},
    {$project: {
        "country": "$country",
        "year": "$yearlyData.year",
        "energy": "$yearlyData.listOfEnergyData",
        "fossil_share": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.fossil_share_energy"  
            as: "c",
            cond: {$ne: ["$$c", []]}}}, 0]}, 0]}
    }}
    
    // Extract lt 100
    {$match: {$expr: {$lt: ["$fossil_share", 100]}}},
    
    {$unwind: "$energy"}
    
    {$project: {
        "year": "$year",
        "country": "$country", 
        "fossil_share": "$fossil_share",
        "energy": "$energy.energy",
        "share_energy" : {$arrayElemAt: [{$filter: {
            input: "$energy.energyTypes",
            as: "c",
            cond: {$eq: ["$$c.type", "share"]}
        }}, 0]}
    }},

    
    {$project: {
        "year": "$year",
        "country": "$country", 
        "fossil_share": "$fossil_share",
        "energy": "$energy",
        "item1": {$arrayElemAt: [{$objectToArray:"$share_energy.energyData"}, 0]},
        "item2": {$arrayElemAt: [{$objectToArray:"$share_energy.energyData"}, 1]}
    }},
    
    {$match: {
        $expr: {
            $and: [
                {$ne: ["$item1.v", 0]}, 
                {$ne: ["$item2.v", 0]}
            ]}
    }},
    
    {$project: {
        "year": "$year",
        "country": "$country", 
        "fossil_share": "$fossil_share",
        "energy": "$energy",
        "energy_share": {
            $cond: {
                if: {$regexFind: {input: "$item1.k" regex:/energy/}},
                then: "$item1.v",
                else: "$item2.v"
            }
        }
    }}, 
    
    {$match: {
        $expr: {
            $and: [
                {$ne: ["$energy", "oil"]},
                {$ne: ["$energy", "coal"]},
                {$ne: ["$energy", "fossil"]},
                {$ne: ["$energy", "gas"]}
                {$ne: ["$energy_share", null]}
                {$ne: ["$energy", "other_renewables_exc_biofuel"]}
            ]
        }
    }

])


//////////////////////////////////               END OF QUESTION 3                ////////////////////////////////////

/*  TOMMY WEE  */

// 4. Compute the average <GDP> of each ASEAN country from 2000 to 2021 (inclusive
// of both years). Display the list of countries based on the descending average GDP
// value.

db.finalOWIDdata.aggregate([
    {$match: {"country": {$in: ["Cambodia", "Indonesia", "Brunei", "Myanmar", "Laos", "Malaysia", "Philippines", "Singapore", "Thailand", "Vietnam"]}}},
    {$unwind: "$yearlyData"},
    {$match: {$and: [{"yearlyData.year":{$gte:2000}}, {"yearlyData.year":{$lte:2021}}]}},
    {$match: {"yearlyData.countryDetails.gdp": {$ne: 0}}},
    {$group: {_id: "$country", "avg_gdp":{$avg: "$yearlyData.countryDetails.gdp"}}},
    {$match: {$expr:{$ne:["$avg_gdp", null]}}},
    {$sort: {"avg_gdp":-1}}
    ])

//////////////////////////////////               END OF QUESTION 4                ////////////////////////////////////

/*  EUGENE WEE  */

// 5. (Without creating additional tables/collections) For each ASEAN country, from 2000
// to 2021 (inclusive of both years), compute the 3-year moving average of
// <oil_consumption> (e.g., 1st: average oil consumption from 2000 to 2002, 2nd:
// average oil consumption from 2001 to 2003, etc.). Based on the 3-year moving
// averages, identify instances of negative changes (e.g., An instance of negative
// change is detected when 1st 3-yo average = 74.232, 2nd 3-yo average = 70.353).
// Based on the pair of 3-year averages, compute the corresponding 3-year moving
// averages in GDP.

db.finalOWIDdata.aggregate([
    
    {$unwind: 
        "$yearlyData"},
    
    // Extract ASEAN countries & between 2000 & 2021
    {$match:{
        $expr: {
            $and: [
                {$in: ["$country", ["Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", 
				                    "Myanmar", "Philippines", "Singapore", "Thailand", "Vietnam"]]},
			    {$and: [
			        {$gt: ["$yearlyData.year", 1999]},
			        {$lt: ["$yearlyData.year", 2022]},
			    ]}
		    ]}
    }},

    // Extract country, year, gdp, oil_consumption (array)
    {$project: {
        "country": "$country",
        "year": "$yearlyData.year",
        "gdp": "$yearlyData.countryDetails.gdp",
        "oil_consumption": {
            $filter: {
                input: "$yearlyData.listOfEnergyData.energyTypes.energyData.oil_consumption",
                as: "oil", 
                cond: {$ne: ["$$oil", []]}
            }}
    }},
    
    // Unwind oil consumption array
    {$unwind: 
        "$oil_consumption"},
    {$unwind: 
        "$oil_consumption"},
    
    // Sort in ascending order year
    {$sort: 
        {"year":1}},
    
    // Group by Countries
    {$group: {
        "_id": {
            "country": "$country",
        }, 
        "yearlyData": {
            $push:{
                "gdp": "$gdp",
                "year": "$year",
                "oil_consumption": "$oil_consumption"
            }
        }
    }},
    
    // 3Y-MA Calculation (Oil)
    {$addFields: {
        "list_oil_3YMA": {
            $map: {
                "input": {
                    $range: [0, {$subtract: [{$size: "$yearlyData"}, 2]}]
                },
                "as": "z",
                "in": {
                    "oil_3YMA": {$avg: {$slice: ["$yearlyData.oil_consumption", "$$z", 3]}},
                    "Period": {$arrayElemAt: ["$yearlyData.year", {$add: ["$$z", 2]}]}
                }
            }
        }  
    }},
    
    // 3Y-MA Calculation (GDP)
    {$addFields: {
        "list_gdp_3YMA": {
            $map: {
                "input": {
                    $range: [0, {$subtract: [{$size: "$yearlyData"}, 2]}]
                },
                "as": "z",
                "in": {
                    "gdp_3YMA": {$avg: {$slice: ["$yearlyData.gdp", "$$z", 3]}},
                    "Period": {$arrayElemAt: ["$yearlyData.year", {$add: ["$$z", 2]}]}
                }
            }
        }  
    }},
    
    // Combine 3YMA for GDP and Oil Cons
    {$unwind: 
        "$list_oil_3YMA"},
    {$unwind: 
        "$list_gdp_3YMA"},
    
    // Extract same period 
    {$match: {
        $expr: {$eq: ["$list_oil_3YMA.Period", "$list_gdp_3YMA.Period"]}
    }},
    
    // Compile 3YMA
    {$project: {
        "_id": {
            "country": "$_id.country"
        },
        "3YMA": {
            "period": "$list_oil_3YMA.Period",
            "oil_3YMA": "$list_oil_3YMA.oil_3YMA",
            "gdp_3YMA": "$list_gdp_3YMA.gdp_3YMA"
        }
    }},
    
    // Calculate yearly 3Y MA changes 
    {$setWindowFields: {
        partitionBy: "$_id.country"
        sortBy: {"3YMA.period":1},   
        output:{
            "diff":{
                $push: "$3YMA",
                window: { range: [-1, "current"]}
            }
        }}
    },
    
    // Calculate 3Y MA differences
    {$group: {
        "_id": "$_id.country",
        "3YMA_difference": {
            $push:{
                "cmp": "$diff"
            }
        }
    }},

    {$unwind: "$3YMA_difference"},
    
    // Check for negative instances (1st instance > 2nd instance)
    {$match: {
        $expr: {
            $and: [
                {$gt: [
                    {$arrayElemAt: ["$3YMA_difference.cmp.oil_3YMA", 0]}, // 1st Instance
                    {$arrayElemAt: ["$3YMA_difference.cmp.oil_3YMA", 1]} // 2nd Instance
                ]},
                {$ne: [{$size: "$3YMA_difference.cmp"}, 1]} 
            ]
        }
    }},
    
    {$unwind: "$3YMA_difference.cmp"},
    
    
    {$project: {
        "_id": 0,
        "country": "$_id",
        "period": {
            $concat: [
                {$toString: {$subtract: ["$3YMA_difference.cmp.period", 2]}}, 
                "-", 
                {$toString: "$3YMA_difference.cmp.period"}
            ]}
        "gdp_3YMA": "$3YMA_difference.cmp.gdp_3YMA"
    }}
    
    // Remove Duplicates
    {$group: {
        "_id": {
            "country": "$country",
            "period_3YMA": "$period"
        },
        "result":{
            $addToSet: {
                "gdp_3YMA": "$gdp_3YMA"      
            }
        }
    }},
    
    {$project:{
        "_id": 0,
        "country": "$_id.country",
        "period_3YMA": "$_id.period_3YMA",
        "gdp_3YMA": {$arrayElemAt: ["$result.gdp_3YMA", 0]}
    }},

    // Get rid of null values 
    {$match: {
        $expr: {$ne: ["$gdp_3YMA", null]}
    }}
    
    // Sorting 
    {$sort: {"country":1, "period_3YMA": 1}}


])


//////////////////////////////////               END OF QUESTION 5                ////////////////////////////////////


/*  SHERMAINE NG  */

// 6. For each <energy_products> and <sub_products>, display the overall average of
// value_ktoe> from [importsofenergyproducts] and [exportsofenergyproducts].

db.finalSGYearlyData.aggregate([
    {$unwind:"$energy_products"},
    {$unwind: "$energy_products.sub_products"},
    {$project: {
        "year":1,
        "name": "$energy_products.name",
        "sub_products": "$energy_products.sub_products.name",
        "exports": "$energy_products.sub_products.exports_value_ktoe",
        "imports": "$energy_products.sub_products.imports_value_ktoe",
    }},
    
    {$group: {
        "_id"  : {
            "name": "$name", 
            "sub_products": "$sub_products"
        },
        "overall_avg_exports_ktoe": {$avg: "$exports"},
        "overall_avg_imports_ktoe": {$avg: "$imports"}
    }}
    ])
    
    
    
//////////////////////////////////               END OF QUESTION 6                ////////////////////////////////////



/*  DENISE TAY  */

// 7. For each combination of <energy_products> and <sub_products>, find the yearly
// difference in <value_ktoe> from [importsofenergyproducts] and
// exportsofenergyproducts]. Identify those years where more than 4 instances of
// export value > import value can be detected.

//question 7 - part 1
db.finalSGYearlyData.aggregate([
    {$unwind: "$energy_products"},
    {$unwind: "$energy_products.sub_products"},
    {$addFields: {"yearly_diff_value_ktoe": {$subtract: ["$energy_products.sub_products.exports_value_ktoe","$energy_products.sub_products.imports_value_ktoe"]} }},

    {$group: {
        "_id": {
            "year": "$year",
            "energy_product": "$energy_products.name",
        },
        "subProductData": {
            $push: {
                "sub_product" : "$energy_products.sub_products.name",
                "yearly_diff_value_ktoe": "$yearly_diff_value_ktoe"
            }
        }
    }}, 
    

    // Group by year > energy product > sub product 
    {$group: {
        "_id": {
            "year": "$_id.year"
        }, 
        "yearData": {
            $push: {
                "energy_product": "$_id.energy_product",
                "sub_product": "$subProductData"
            }
        }
    }},
    
    //Sort by ascending year 
    {$sort: {"_id.year": 1}}
    ])


//question 7 - part 2
/*Identify those years where more than 4 instances of export value > import value can be detected.*/
db.finalSGYearlyData.aggregate([
    {$unwind: "$energy_products"},
    {$unwind: "$energy_products.sub_products"},
    {$project: {_id:0, year:1, yearly_diff_value_ktoe: {$subtract: ["$energy_products.sub_products.exports_value_ktoe","$energy_products.sub_products.imports_value_ktoe"]}} },
    {$match:{ yearly_diff_value_ktoe: {$gt: 0}}}, 
    {$group: {
      _id: '$year',
      yearCount: { $count: {} }}},
    {$match:{ yearCount: {$gt: 4}}}, 
    ])
    

//////////////////////////////////               END OF QUESTION 7                ////////////////////////////////////

/*  TOMMY WEE  */

// 8. In [householdelectricityconsumption], for each <region>, excluding “overall”, generate
// the yearly average <kwh_per_acc>.

db.finalSGYearlyData.aggregate([

    // Extract Annual Data
    {$set:{
        "annual": {$arrayElemAt: [{
            $filter: {
                input: "$monthlyHouseholdConsumption",
                as: "c",
                cond: {$eq: ["$$c.month", "Annual"]}
            }
        }, 0]}
    }},
    
    // Extract Overall Household
     {$set:{
        "result": {$arrayElemAt: [{
            $filter: {
                input: "$annual.household_category",
                as: "c",
                cond: {$eq: ["$$c.household_type", "Overall"]}
            }
        }, 0]}
    }},
    
    {$unwind: "$result.regional_elect_data"},
    
    // Exclude Overall
    {$match: {
        $expr: {$ne: ["$result.regional_elect_data.Region", "Overall"]}
    }},
    
    {$project: {
        "region": "$result.regional_elect_data.Region", 
        "yearlyData": {
            "year": "$year", 
            "yearly_avg_kwh_per_acc": "$result.regional_elect_data.region_kwh_per_acc"
        }
    }}
    
    {$group: {
        _id: "$region",
        "yearlyData": {
            $push: "$yearlyData"
        }
    }
])


//////////////////////////////////               END OF QUESTION 8                ////////////////////////////////////

/*  SHERMAINE NG  */

// 9. Who are the energy-saving stars? Compute the yearly average of <kwh_per_acc> in
// each region, excluding “overall”. Generate the moving 2-year average difference (i.e.,
// year 1 average kwh_per_acc for the central region = 1223, year 2 = 1000, the
// moving 2-year average difference = -223). Display the top 3 regions with the most
// instances of negative 2-year averages.


// Yearly Average in each region (excld Overall)
db.finalSGYearlyData.aggregate([

    // Extract Annual Data
    {$set:{
        "annual": {$arrayElemAt: [{
            $filter: {
                input: "$monthlyHouseholdConsumption",
                as: "c",
                cond: {$eq: ["$$c.month", "Annual"]}
            }
        }, 0]}
    }},
    
    // Extract Overall Household
     {$set:{
        "result": {$arrayElemAt: [{
            $filter: {
                input: "$annual.household_category",
                as: "c",
                cond: {$eq: ["$$c.household_type", "Overall"]}
            }
        }, 0]}
    }},
    
    {$unwind: 
        "$result.regional_elect_data"},
    
    // Exclude Overall
    {$match: {
        $expr: {$ne: ["$result.regional_elect_data.Region", "Overall"]}
    }},
    
    {$project: {
        "region": "$result.regional_elect_data.Region", 
        "yearlyData": {
            "year": "$year", 
            "yearly_avg_kwh_per_acc": "$result.regional_elect_data.region_kwh_per_acc"
        }
    }}
])


// Generate the moving 2-year average difference 
db.finalSGYearlyData.aggregate([

    // Extract Annual Data
    {$set:{
        "annual": {$arrayElemAt: [{
            $filter: {
                input: "$monthlyHouseholdConsumption",
                as: "c",
                cond: {$eq: ["$$c.month", "Annual"]}
            }
        }, 0]}
    }},
    
    // Extract Overall Household
     {$set:{
        "result": {$arrayElemAt: [{
            $filter: {
                input: "$annual.household_category",
                as: "c",
                cond: {$eq: ["$$c.household_type", "Overall"]}
            }
        }, 0]}
    }},
    
    {$unwind:
        "$result.regional_elect_data"},
    
    // Exclude Overall
    {$match: {
        $expr: {$ne: ["$result.regional_elect_data.Region", "Overall"]}
    }},
    
    {$project: {
        "region": "$result.regional_elect_data.Region", 
        "yearlyData": {
            "year": "$year", 
            "yearly_avg_kwh_per_acc": "$result.regional_elect_data.region_kwh_per_acc"
        }
    }},
    
    {$group: {
        _id: "$region",
        "yearlyData": {
            $push: "$yearlyData"
        }
    }},
    
    {$addFields: {
        "list_kwh_per_acc_2YMA": {
            $map: {
                "input": {
                    $range: [0, {$size : "$yearlyData"}]},
            "as" : "z",
            "in" : {
                "kwh_per_acc_2YMA" : {$slice: ["$yearlyData.yearly_avg_kwh_per_acc", "$$z", 2]},
                "Period" : {$arrayElemAt: ["$yearlyData.year",
                "$$z"]}}
            }
        }
    }},
    {$unwind:"$list_kwh_per_acc_2YMA"},
    {$project: {
      _id:1, 
      "period": "$list_kwh_per_acc_2YMA.Period"
      "2YMA": {$subtract: [{$arrayElemAt: ["$list_kwh_per_acc_2YMA.kwh_per_acc_2YMA", 1]},
                                            {$arrayElemAt: ["$list_kwh_per_acc_2YMA.kwh_per_acc_2YMA", 0]}]
    }}},
    
  {$project: {
    _id:1, 
    "period": {$concat: [{$toString: "$period"}, " - ", {$toString: {$add: ["$period", 1]}}]}
    "diff":  "$2YMA"
  }}
  
  {$match: {$expr:{
      $and: [{$lt: ["$diff", 0]},
            {$ne: ["$diff", null]}]
  }}}
  
])


// Generate the regions with highest num of negative instances 
db.finalSGYearlyData.aggregate([

    // Extract Annual Data
    {$set:{
        "annual": {$arrayElemAt: [{
            $filter: {
                input: "$monthlyHouseholdConsumption",
                as: "c",
                cond: {$eq: ["$$c.month", "Annual"]}
            }
        }, 0]}
    }},
    
    // Extract Overall Household
     {$set:{
        "result": {$arrayElemAt: [{
            $filter: {
                input: "$annual.household_category",
                as: "c",
                cond: {$eq: ["$$c.household_type", "Overall"]}
            }
        }, 0]}
    }},
    
    {$unwind:
        "$result.regional_elect_data"},
    
    // Exclude Overall
    {$match: {
        $expr: {$ne: ["$result.regional_elect_data.Region", "Overall"]}
    }},
    
    {$project: {
        "region": "$result.regional_elect_data.Region", 
        "yearlyData": {
            "year": "$year", 
            "yearly_avg_kwh_per_acc": "$result.regional_elect_data.region_kwh_per_acc"
        }
    }},
    
    {$group: {
        _id: "$region",
        "yearlyData": {
            $push: "$yearlyData"
        }
    }},
    
    {$addFields: {
        "list_kwh_per_acc_2YMA": {
            $map: {
                "input": {
                    $range: [0, {$size : "$yearlyData"}]},
            "as" : "z",
            "in" : {
                "kwh_per_acc_2YMA" : {$slice: ["$yearlyData.yearly_avg_kwh_per_acc", "$$z", 2]},
                "Period" : {$arrayElemAt: ["$yearlyData.year",
                "$$z"]}}
            }
        }
    }},
    {$unwind:"$list_kwh_per_acc_2YMA"},
    {$project: {
      _id:1, 
      "period": "$list_kwh_per_acc_2YMA.Period"
      "2YMA": {$subtract: [{$arrayElemAt: ["$list_kwh_per_acc_2YMA.kwh_per_acc_2YMA", 1]},
                                            {$arrayElemAt: ["$list_kwh_per_acc_2YMA.kwh_per_acc_2YMA", 0]}]
    }}},
    
  {$project: {
    _id:1, 
    "period": {$concat: [{$toString: "$period"}, " - ", {$toString: {$add: ["$period", 1]}}]}
    "diff":  "$2YMA"
  }}
  
  {$match: {$expr:{
      $and: [{$lt: ["$diff", 0]},
            {$ne: ["$diff", null]}]
  }}}
 
    {$group: {
      _id: "$_id",
      "count": {
          $sum: 1
      }
    }}
  
])

//////////////////////////////////               END OF QUESTION 9                ////////////////////////////////////

/*  EUGENE WEE  */

// 10. Are there any seasonal (quarterly) effects on energy consumption? Visualizations are
// typically required to eyeball the effects. For each region, in each year, compute the
// quarterly average in <kwh_per_acc>. Exclude “Overall” in <region>.
// Note: 1st quarter = January, February, and March, 2nd quarter = April, May, and June,
// and so on.

db.finalSGYearlyData.aggregate([

    // Unwind & filter overall household type
    {$unwind: 
        "$monthlyHouseholdConsumption"}
    {$project: {
        "month": "$monthlyHouseholdConsumption.month",
        "year": "$year",
        "result" : {$arrayElemAt: [
                        {$filter: {
                            input: "$monthlyHouseholdConsumption.household_category",
                            as: "r",
                            cond: {$eq: ["$$r.household_type", "Overall"]}
                        }}, 0]}
    }},
    
    // Unwind regional data
    {$unwind: 
        "$result.regional_elect_data"},
    
    // Exclude Overall in Region
    {$match: {
        $expr: {$ne: ["$result.regional_elect_data.Region", "Overall"]  
    }}},
    
    // Categorise into Quarter
    {$addFields:{
        "quarter": {
            $cond: {
                if: {$lt: ["$month", 4]},
                then: 1,
                else: {$cond: {
                    if: {$lt: ["$month", 7]},
                    then: 2,
                    else: {$cond: {
                        if: {$lt: ["$month", 10]},
                        then: 3,
                        else: 4
                    }
                }
            }
        }
    }}}}
    
    // Calculate quarterly average by Region by Year 
    {$group: {
        "_id":{
            "quarter": "$quarter",
            "year": "$year",
            "region": "$result.regional_elect_data.Region"
        }, 
        "quarterly_avg_kwh": {$avg: "$result.regional_elect_data.region_kwh_per_acc"}
    }},
    
    // Create Array for Quarter
    {$group: {
        "_id": {
            "region": "$_id.region",
            "year": "$_id.year"
        }
        "quarter": {
            $push: {
                "quarter": "$_id.quarter", 
                "quarterly_avg_kwh": "$quarterly_avg_kwh"
            }
        }
    }},
    
    // Sort Ascending Quarter
    {$project: {
        _id: 1, 
        quarter: {
            $sortArray: {
                input: "$quarter", 
                sortBy: {"quarter": 1}
            }
        }
    }},
    
    // Group by Region > Year > Quarter
    {$group: {
        "_id": {
            "region": "$_id.region"
        }, 
        "yearData": {
            $push: {
                "year": "$_id.year",
                "quarter": "$quarter"
            }
        }
    }},
    
    // Sort Ascending Year
    {$project: {
        "_id": 0,
        "region": "$_id.region",
        "year": {
            $sortArray: {
                input: "$yearData",
                sortBy: {"year": 1, "quarter": 1}
            }, 
        }
    }}

])



//////////////////////////////////               END OF QUESTION 10                ////////////////////////////////////


/*  DENISE TAY  */

// 11. Consider [householdtowngasconsumption]. Are there any seasonal (quarterly) effects
// on town gas consumption? For each <sub_housing_type>, in each year, compute the
// quarterly average in <avg_mthly_hh_tg_consp_kwh>. Exclude “Overall” in <
// sub_housing_type>.

db.finalSGYearlyData.aggregate( [
    {$unwind: "$monthlyHouseholdConsumption"},
    {$addFields:{
        "quarter" :
        {
          $switch:
            {
              branches: [
                {
                  case: { $and : [ { $gte : [ "$monthlyHouseholdConsumption.month" , 1 ] },
                                   { $lte : [ "$monthlyHouseholdConsumption.month" , 3 ] } ] },
                  then: "1"
                },
                {
                  case: { $and : [ { $gte : [ "$monthlyHouseholdConsumption.month" , 4 ] },
                                   { $lte : [ "$monthlyHouseholdConsumption.month" , 6 ] } ] },
                  then: "2"
                },
                {
                  case: { $and : [ { $gte : [ "$monthlyHouseholdConsumption.month" , 7 ] },
                                   { $lte : [ "$monthlyHouseholdConsumption.month" , 9 ] } ]},
                  then: "3"
                },
                {
                  case: { $and : [ { $gte : [ "$monthlyHouseholdConsumption.month" , 10 ] },
                                   { $lte : [ "$monthlyHouseholdConsumption.month" , 12 ] } ] },
                  then: "4"
                }
              ],
              default: "yearlyTotal"
            }
         }
      }
   }, 
   
   {$match: { "quarter": {$ne: "yearlyTotal"}}}, 
   
   {$unwind: "$monthlyHouseholdConsumption.household_category"},
   
   {$group: {
       "_id":{
            "quarter": "$quarter",
            "year": "$year",
            "sub_housing_type": "$monthlyHouseholdConsumption.household_category.household_type"},
      
      avg_qtrly_hh_tg_consp_kwh: { $avg: "$monthlyHouseholdConsumption.household_category.avg_mthly_hh_tg_consp_kwh" }} },
      
    // Create Array for Quarter
    {$group: {
        "_id": {
            "sub_housing_type": "$_id.sub_housing_type",
            "year": "$_id.year"
},
        "quarter": {
            $push: {
                "quarter": "$_id.quarter", 
                "avg_qtrly_hh_tg_consp_kwh": "$avg_qtrly_hh_tg_consp_kwh"
            }
        }
    }},
    
    // Sort Ascending Quarter
    {$project: {
        _id: 1, 
        quarter: {
            $sortArray: {
                input: "$quarter", 
                sortBy: {"quarter": 1}
            }
        }
    }},
    
    // Group by Sub housing type > Year > Quarter
    {$group: {
        "_id": {
            "sub_housing_type": "$_id.sub_housing_type"
        }, 
        "yearData": {
            $push: {
                "year": "$_id.year",
                "quarter": "$quarter"
            }
        }
    }},
    
    // Sort Ascending Year
    {$project: {
        "_id": 0,
        "sub_housing_type": "$_id.sub_housing_type",
        "year": {
            $sortArray: {
                input: "$yearData",
                sortBy: {"year": 1, "quarter": 1}
            }, 
        }
    }},
    
    {$match: {$and : [{"sub_housing_type": {$ne: "Overall"} }, {"sub_housing_type": {$ne: "PublicHousing"} }, {"sub_housing_type": {$ne: "PrivateHousing"} } ]}},
    
])


//////////////////////////////////               END OF QUESTION 11                ////////////////////////////////////


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////               OPEN ENDED QUERIES               ////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



/*  EUGENE WEE  */

// 13. Can renewable energy adequately power continued economic growth? 

// ## Sustaining Energy Demand 
// Figure: World Population, Energy & Electricity Demand since 2000

db.finalOWIDdata.aggregate([
        
    // Unwind data
    {$unwind: "$yearlyData"},
    
    // Extract World Data & Year since 2000
    {$match: {
        $expr: {
            $and: [
                {$eq: ["$country", "World"]}, 
                {$gte: ["$yearlyData.year", 2000]}
            ]}
    }}, 

    
    // Project year, population, electricity demand, primary_energy_consumption
    {$project: {
        "_id": 0,
        "year": "$yearlyData.year", 
        "population": "$yearlyData.countryDetails.population",
        "electricity_demand": "$yearlyData.countryEnergyData.electricity_demand",
        "primary_energy_consumption": "$yearlyData.countryEnergyData.primary_energy_consumption"
    }},
    
    {$sort: {"year": 1}}
])

    
// Figure: World Population, Energy & Electricity Demand by Type since 2000

db.finalOWIDdata.aggregate([
    
    // Unwind data
    {$unwind: "$yearlyData"},
    
    // Extract World Data & Year for 2000 and 2020
    {$match: {
        $expr: {
            $and: [
                {$eq: ["$country", "World"]}, 
                {$in: ["$yearlyData.year", [2000, 2020]]}
            ]}
    }}, 
    
    // Project results 
    {$project: {
        "_id": 0,
        "year": "$yearlyData.year", 
        "total_elec": "$yearlyData.countryEnergyData.primary_energy_consumption",
        "fossil_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.fossil_fuel_consumption",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "renewable_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.renewables_consumption",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "other_renew_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.other_renewable_consumption",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "nuclear_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.nuclear_consumption",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]}
    }},
    
    {$project: {
        "_id": "$year",
        "total_elec": 1, 
        "fossil_elec": 1, 
        "renewable_elec": {$add: ["$renewable_elec", "$other_renew_elec"]},
        "nuclear_elec": 1,
    }}
])


// Figure: Percentage Change in Energy Consumption by Type Since 2000   
db.finalOWIDdata.aggregate([
    
    // Unwind data
    {$unwind: "$yearlyData"},
    
    // Extract World Data & Year for 2000 onwards
    {$match: {
        $expr: {
            $and: [
                {$eq: ["$country", "World"]}, 
                {$gte: ["$yearlyData.year", 2000]}
            ]}
    }}, 
    
    // Project results 
    {$project: {
        "_id": {
            "year": "$yearlyData.year"
        },
        "solar_cons_change_pct": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.solar_cons_change_pct",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "wind_cons_change_pct": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.wind_cons_change_pct",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "biofuel_cons_change_pct": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.biofuel_cons_change_pct",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "hydro_cons_change_pct": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.hydro_cons_change_pct",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "nuclear_cons_change_pct": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.nuclear_cons_change_pct",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "other_renewables_cons_change_pct": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.other_renewables_cons_change_pct",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]}
    }},
    
    {$sort: {"_id.year": 1}}
    
])




// ## Adequate Energy Generation 

// Figure: Breakdown of energy by type (2020 since 2021 no data)
db.finalOWIDdata.aggregate([
    
    // Unwind data
    {$unwind: "$yearlyData"},
    
    // Extract World Data & Year for 2020
    {$match: {
        $expr: {
            $and: [
                {$eq: ["$country", "World"]}, 
                {$eq: ["$yearlyData.year", 2020]}
            ]}
    }}, 
    
    // Project results 
    {$project: {
        "_id": {
            "year": "$yearlyData.year"
        },
        
        "fossil_share_energy": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.fossil_share_energy",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "solar_share_energy": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.solar_share_energy",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "wind_share_energy": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.wind_share_energy",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "biofuel_share_energy": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.biofuel_share_energy",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "hydro_share_energy": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.hydro_share_energy",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "nuclear_share_energy": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.nuclear_share_energy",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "other_renewables_share_energy": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.other_renewables_share_energy",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]}
    }}
])


// Figure: Breakdown of electricity by type (2021)
db.finalOWIDdata.aggregate([
    
    // Unwind data
    {$unwind: "$yearlyData"},
    
    // Extract World Data & Year for 2021
    {$match: {
        $expr: {
            $and: [
                {$eq: ["$country", "World"]}, 
                {$eq: ["$yearlyData.year", 2021]}
            ]}
    }}, 
    
    // Project results 
    {$project: {
        "_id": {
            "year": "$yearlyData.year"
        },
        "fossil_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.fossil_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "solar_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.solar_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "wind_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.wind_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "biofuel_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.biofuel_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "hydro_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.hydro_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "nuclear_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.nuclear_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "other_renewables_share_elec_exc_biofuel": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.other_renewables_share_elec_exc_biofuel",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]}
    }}
])



// Figure: World Energy Generation (per capita) 
db.finalOWIDdata.aggregate([
    
    // Unwind data
    {$unwind: "$yearlyData"},
    
    // Extract World Data & Year for 2000 onwards
    {$match: {
        $expr: {
            $and: [
                {$eq: ["$country", "World"]}, 
                {$gte: ["$yearlyData.year", 2000]}
            ]}
    }}, 
    
    // Project results 
    {$project: {
        "_id": {
            "year": "$yearlyData.year"
        },
        "per_capita_electricity": "$yearlyData.countryEnergyData.per_capita_electricity",
        "coal_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.coal_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "gas_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.gas_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "oil_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.oil_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "solar_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.solar_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "wind_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.wind_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "biofuel_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.biofuel_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "hydro_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.hydro_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "other_renewables_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.other_renewables_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "nuclear_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.nuclear_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]}
    }},
    
    {$project: {
        "per_capita_electricity": 1,
        "fossil_elect_per_capita": {$add: ["$coal_elec_per_capita", "$gas_elec_per_capita", "$oil_elec_per_capita"]}
        "renewables_elec_per_capita": {$add: ["$hydro_elec_per_capita", "$other_renewables_elec_per_capita",
                                            "$biofuel_elec_per_capita", "$wind_elec_per_capita", "$solar_elec_per_capita"]},
        "nuclear_elec_per_capita": 1
    }},
    
    {$sort: {"_id.year": 1}}
    
])



// Figure: World Electricity generation by energy types
db.finalOWIDdata.aggregate([
    
    // Unwind data
    {$unwind: "$yearlyData"},
    
    // Extract World Data & Year for 2000 onwards
    {$match: {
        $expr: {
            $and: [
                {$eq: ["$country", "World"]}, 
                {$gte: ["$yearlyData.year", 2000]}
            ]}
    }}, 
    
    // Project results 
    {$project: {
        "_id": {
            "year": "$yearlyData.year"
        },
        "electricity_demand": "$yearlyData.countryEnergyData.electricity_demand",
        "solar_electricity": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.solar_electricity",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "wind_electricity": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.wind_electricity",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "hydro_electricity": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.hydro_electricity",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "other_renewable_electricity": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.other_renewable_electricity",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]}
    }},
    
    {$sort: {"_id.year": 1}}
    
])



// Figure: Electricity Source by Continent (‘00 vs ‘21)
db.finalOWIDdata.aggregate([
    
    // Filter by Countries
    {$lookup: {
        from: "countriesOWID",
        localField: "country",
        foreignField: "Entity",
        as: "OWIDcountries"
    }},
    
    // Unwind data
    {$unwind: "$yearlyData"},
    
    // Extract World Data & Year for 2000 onwards
    {$match: {
        $expr: {
            $and: [
                {$ne: ["$OWIDcountries", []]}, 
                {$gte: ["$yearlyData.year", 2000]}
            ]}
    }}, 
    
    // Project data 
    {$project: {
        "_id": {
            "year": "$yearlyData.year"
        },
        "OWIDcountries": 1
        "per_capita_electricity": "$yearlyData.countryEnergyData.per_capita_electricity",
        "coal_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.coal_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "gas_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.gas_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "oil_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.oil_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []}
                        }}, 0]}, 0]},
        "solar_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.solar_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "wind_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.wind_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "biofuel_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.biofuel_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "hydro_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.hydro_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "other_renewables_elec_per_capita_exc_biofuel": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.other_renewables_elec_per_capita_exc_biofuel",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "nuclear_elec_per_capita": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.nuclear_elec_per_capita",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]}
    }},
    
    {$project: {
        "_id": 1,
        "Continent": {$arrayElemAt: ["$OWIDcountries.Continent", 0]}
        "per_capita_electricity": 1,
        "fossil_elect_per_capita": {$add: ["$coal_elec_per_capita", "$gas_elec_per_capita", "$oil_elec_per_capita"]}
        "renewables_elec_per_capita": {$add: ["$hydro_elec_per_capita", "$other_renewables_elec_per_capita_exc_biofuel",
                                            "$biofuel_elec_per_capita", "$wind_elec_per_capita", "$solar_elec_per_capita"]},
        "nuclear_elec_per_capita": 1
    }},
    
    // Group by continents
    {$group: {
        "_id": {
            "year": "$_id.year",
            "continent": "$Continent"
        },
        "fossil_elect_per_capita": {$avg: "$fossil_elect_per_capita"},
        "renewables_elec_per_capita": {$avg: "$renewables_elec_per_capita"},
        "nuclear_elec_per_capita": {$avg: "$nuclear_elec_per_capita"}
    }},
    
    {$sort: {"_id.continent": 1, "_id.year": 1}}

])



// Figure: Extracting countries with highest consumption of renewables vs electricity demand

db.finalOWIDdata.aggregate([
    
    // Filter by Country
    {$lookup: {
        from: "countriesOWID",
        localField: "country",
        foreignField: "Entity",
        as: "OWIDcountries"
    }},
    
    // Unwind data
    {$unwind: "$yearlyData"},
    
    // Extract World Data & Year for 2000 onwards
    {$match: {
        $expr: {
            $and: [
                {$ne: ["$OWIDcountries", []]}, 
                {$gte: ["$yearlyData.year", 2000]}
            ]}
    }}, 
    
    // Project necessary info
    {$project:{
        _id:0,
        "year": "$yearlyData.year",
        "country": "$country",
        "renewables_consumption": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.renewables_consumption",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        "electricity_demand": "$yearlyData.countryEnergyData.electricity_demand"
    }},
    
    // Add new renewables field
    {$addFields: {
        "renewables": {$cond: {
            if: {$ne: ["$electricity_demand", 0]},
            then: {$divide: ["$renewables_consumption", "$electricity_demand"] },
            else: 0}}
    }},

    // Group by average renewables
    {$group:{
        "_id": "$country",
        "renewables_pct": {
            $avg: "$renewables"
        }
    }},
    
        
    // Sort by descending order renewables & extract top 5 
    {$sort: {"renewables_pct": -1}},
    {$limit: 5},
    
    
    // Join owid energy
    {$lookup: {
        from: "finalOWIDdata",
        localField: "_id",
        foreignField: "country",
        as: "OWIDdata"
    }},
    
    {$unwind: "$OWIDdata"},
    {$unwind: "$OWIDdata.yearlyData"}
    
    // Extract Year for 2000 onwards
    {$match: {
        $expr: 
            {$gte: ["$OWIDdata.yearlyData.year", 2000]}
        
    }}, 
    
    {$project: {
        _id:0,
        "country": "$_id",
        "year": "$OWIDdata.yearlyData.year",
        "gdp": "$OWIDdata.yearlyData.countryDetails.gdp",
        "renewables_consumption": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$OWIDdata.yearlyData.listOfEnergyData.energyTypes.energyData.renewables_consumption",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]}
    }},
    
    {$sort: {"country": 1, "year": 1}}
        
])

//////////////////////////////////               END OF QUESTION 13                ////////////////////////////////////


/*  DENISE TAY  */
//Question 14 noSQL queries 

//2010-2020 Singapore's Energy product Trade

db.finalSGYearlyData.aggregate([
    {$unwind: "$energy_products"},
    {$unwind: "$energy_products.sub_products"},
    
    //Sum yearly export and imports 
    {$group: {
       "_id":{
            "year": "$year"
           },
      
      total_import_value_ktoe: { $sum: "$energy_products.sub_products.imports_value_ktoe" }, 
        total_export_value_ktoe: { $sum: "$energy_products.sub_products.exports_value_ktoe" }, 
    } },
    
    //Sort by ascending year 
    {$sort: {"_id.year": 1}}
    ])

//---------------------------------------

//2010-2020 Singapore's Balance of Energy Product Trade

db.finalSGYearlyData.aggregate([
    {$unwind: "$energy_products"},
    {$unwind: "$energy_products.sub_products"},
    //Sum yearly export and imports 
    {$group: {
       "_id":{
            "year": "$year"
           },
      
      total_import_value_ktoe: { $sum: "$energy_products.sub_products.imports_value_ktoe" }, 
        total_export_value_ktoe: { $sum: "$energy_products.sub_products.exports_value_ktoe" }, 
    } },
    {$project: {"_id":1, "yearly_value_ktoe": {$subtract: ["$total_export_value_ktoe","$total_import_value_ktoe"]}}},
    //Sort by ascending year 
    {$sort: {"_id.year": 1}}
    ])
 
//---------------------------------------

//2010-2020 Low Carbon Electricity By Type
db.finalOWIDdata.aggregate([
    

    {$unwind: "$yearlyData"},
    

    {$match: {
        $expr: {
            $and: [
                {$eq: ["$country", "World"]}, 
                {$gte: ["$yearlyData.year", 2000]}
            ]}
    }}, 
    

    {$project: {
        "_id": {
            "year": "$yearlyData.year"
        },
        "solar_electricity": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.solar_electricity",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        "wind_electricity": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.wind_electricity",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        "hydro_electricity": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.hydro_electricity",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        "nuclear_electricity": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.nuclear_electricity",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        "other_renewable_electricity": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.other_renewable_electricity",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        
    }},
    
    {$sort: {"_id.year": 1}}
])

//---------------------------------------
// 2000 & 2020 Singapore electricity mix
db.finalOWIDdata.aggregate([
    

    {$unwind: "$yearlyData"},
    

    {$match: {
        $expr: {
            $and: [
                {$eq: ["$country", "Singapore"]}, 
                {$in: ["$yearlyData.year", [2000, 2020]]}
            ]}
    }}, 
    

    {$project: {
        "_id": {
            "year": "$yearlyData.year"
        },
        "solar_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.solar_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        "fossil_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.fossil_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        
        "other_renewables_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.other_renewables_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        
    }},
    
    {$sort: {"_id.year": 1}}
])


//---------------------------------------
        
//2020 Global Energy Mix
db.finalOWIDdata.aggregate([
    

    {$unwind: "$yearlyData"},
    

    {$match: {
        $expr: {
            $and: [
                {$eq: ["$country", "World"]}, 
                {$eq: ["$yearlyData.year", 2020]}
            ]}
    }}, 
    

    {$project: {
        "_id": {
            "year": "$yearlyData.year"
        },
        "renewables_share_energy": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.renewables_share_energy",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        "fossil_share_energy": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.fossil_share_energy",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        "nuclear_share_energy": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.nuclear_share_energy",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        "other_renewables_share_energy": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.other_renewables_share_energy",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        
    }},
    
    {$sort: {"_id.year": 1}}
])

//---------------------------------------
        
//2020 Global Electricity Mix
db.finalOWIDdata.aggregate([
    

    {$unwind: "$yearlyData"},
    

    {$match: {
        $expr: {
            $and: [
                {$eq: ["$country", "World"]}, 
                {$eq: ["$yearlyData.year", 2020]}
            ]}
    }}, 
    

    {$project: {
        "_id": {
            "year": "$yearlyData.year"
        },
        "renewables_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.renewables_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        "fossil_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.fossil_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        "nuclear_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.nuclear_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                        }}, 0]}, 0]},
        "other_renewables_share_elec": {$arrayElemAt: [{$arrayElemAt: [{$filter: {
                            input: "$yearlyData.listOfEnergyData.energyTypes.energyData.other_renewables_share_elec",
                            as: "c", 
                            cond: {$ne: ["$$c", []]}
                            }}, 0]}, 0]},
        
    }},
    
    {$sort: {"_id.year": 1}}
])


//////////////////////////////////               END OF QUESTION 14                ////////////////////////////////////


//////////////////////////////////               END OF PROJECT QUERY               ////////////////////////////////////

