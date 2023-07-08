
use sk10689

// Q1 Write MongoDB queries for (2 points each):
// Q1
print("\nQ1-Count the number of documents in the collection.")
print(db.restaurants.count())

// Q2
print("\nQ2-Display all the documents in the collection.")
db.restaurants.find().forEach(printjson)

// Q3
print("\nQ3-Display: restaurant_id, name, borough and cuisine for all the documents.")
db.restaurants.find({}, {_id: 1, restaurant_id: 1, name: 1, borough: 1, cuisine: 1}).forEach(printjson)

// Q4
print("\nQ4-Display: restaurant_id, name, borough and cuisine, but exclude field _id, for all the documents in the collection.")
db.restaurants.find({}, {_id: 0, restaurant_id: 1, name: 1, borough: 1, cuisine: 1}).forEach(printjson)

// Q5
print("\nQ5-Display: restaurant_id, name, borough and zip code, exclude the field _id for all the documents in the collection.")
db.restaurants.find({}, {_id: 0, restaurant_id: 1, name: 1, borough: 1, "address.zipcode": 1}).forEach(printjson)

// Q6
print("\nQ6-Display all the restaurants in the Bronx.")
db.restaurants.find({"borough": "Bronx"}).forEach(printjson)

// Q7
print("\nQ7-Display the first 5 restaurants in the Bronx.")
db.restaurants.find({"borough": "Bronx"}).limit(5).forEach(printjson)

// Q8
print("\nQ8-Display the second 5 restaurants (skipping the first 5) in the Bronx.")
db.restaurants.find({"borough": "Bronx"}).skip(5).limit(5).forEach(printjson)

// Q9 For assumption of individual score
print("\nQ9-Find the restaurants with any score more than 85.")
db.restaurants.find({"grades.score": {"$gt": 85}}).forEach(printjson)
// For assumption of sum of scores
// db.restaurants.find({"$expr": {"$gt": [{"$sum": "$grades.score"}, 85]}}).forEach(printjson)

// Q10 For assumption of individual score
print("\nQ10-Find the restaurants that achieved a score, more than 80 but less than 100.")
db.restaurants.find({"grades": {$elemMatch: {"score": {"$gt": 80, "$lt": 100}}}}).forEach(printjson)
// For assumption of sum of scores
/*db.restaurants.find({"$and": [
 {"$expr": {"$gt": [{"$sum": "$grades.score"}, 80]}},
 {"$expr": {"$lt": [{"$sum": "$grades.score"}, 100]}}
]}).forEach(printjson)*/

// Q11 For assumption of 1st coordinate is latitude and 2nd coordinate is longitude as per your comments on slack
print("\nQ11-Find the restaurants which locate in longitude value less than -95.754168.")
db.restaurants.find({"address.coord.1": {"$lt": -95.754168}}).forEach(printjson)
// no documents satisfy the above query conditions as no valid longitude is less than -95.754168 in the collection

// Q12 For assumption of 1st coordinate is latitude and 2nd coordinate is longitude as per your comments on slack
print("\nQ12-Find the restaurants that do not prepare any cuisine of 'American' and their grade score more than 70 and longitude less than -65.754168.")
db.restaurants.find({"$and": [
 {"cuisine": {"$ne": "American "}},
 {"grades.score": {"$gt": 70}},
 {"address.coord.1": {"$lt": -65.754168}}
]}).forEach(printjson)
//no documents satisfy the above query conditions as no longitude is less than -65.754168 in the collection

// Q13 For assumption of 1st coordinate is latitude and 2nd coordinate is longitude as per your comments on slack
print("\nQ13-Find the restaurants which do not prepare any cuisine of 'American' and achieved a score more than 70 and located in the longitude less than -65.754168. (without using $and operator).")
db.restaurants.find({
 "cuisine": {"$ne" : "American "},
 "grades.score": {"$gt": 70},
 "address.coord.0": {"$lt": -65.754168}
}).forEach(printjson)

// Q14
print("\nQ14-Find the restaurants which do not prepare any cuisine of 'American ' and achieved a grade point 'A' and not in the borough of Brooklyn, sorted by cuisine in descending order.")
db.restaurants.find({"$and": [
 {"cuisine": {"$ne": "American "}},
 {"grades.grade": "A"},
 {"borough": {"$ne": "Brooklyn"}}
]}).sort({"cuisine": -1}).forEach(printjson)

// Q15
print("\nQ15-Find the restaurant Id, name, borough and cuisine for those restaurants which contain 'Wil' as first three letters for its name.")
db.restaurants.find({"name": {$regex: /^Wil.*/}}, {_id: 0, restaurant_id: 1, name: 1, borough: 1, cuisine: 1}).forEach(printjson)

// Q16
print("\nQ16-Find the restaurant Id, name, borough and cuisine for those restaurants which contain 'ces' as last three letters for its name.")
db.restaurants.find({"name": {$regex: /.*ces$/}}, {_id: 0, restaurant_id: 1, name: 1, borough: 1, cuisine: 1}).forEach(printjson)

// Q17
print("\nQ17-Find the restaurant Id, name, borough and cuisine for those restaurants which contain 'Reg' as three letters somewhere in its name.")
db.restaurants.find({"name": {$regex: /Reg/}}, {_id: 0, restaurant_id: 1, name: 1, borough: 1, cuisine: 1}).forEach(printjson)

// Q18
print("\nQ18-Find the restaurants which belong to the borough Bronx and prepared either American or Chinese dish.")
db.restaurants.find({"borough": "Bronx", "cuisine": {$in: ["American ", "Chinese"]}}).forEach(printjson)

// Q19
print("\nQ19-Find the restaurant Id, name, borough and cuisine for those restaurants which belong to the boroughs of Staten Island or Queens or Bronx or Brooklyn.")
db.restaurants.find({"borough": {$in: ["Staten Island", "Queens", "Bronx", "Brooklyn"]}}, {_id: 0, restaurant_id: 1, name: 1, borough: 1, cuisine: 1}).forEach(printjson)

// Q20
print("\nQ20-Find the restaurant Id, name, borough and cuisine for those restaurants which are not belonging to the borough Staten Island or Queens or Bronx or Brooklyn.")
db.restaurants.find({"borough": {$nin: ["Staten Island", "Queens", "Bronx", "Brooklyn"]}}, {_id: 0, restaurant_id: 1, name: 1, borough: 1, cuisine: 1}).forEach(printjson)

// Q21
print("\nQ21-Find the restaurant Id, name, borough and cuisine for those restaurants which achieved a score below 10.")
db.restaurants.find({"grades.score": {"$lt": 10}}, {_id: 0, restaurant_id: 1, name: 1, borough: 1, cuisine: 1}).forEach(printjson)

// Q22
print("\nQ22-Find the restaurant Id, name, borough and cuisine for those restaurants which prepared dish except 'American' and 'Chinese' or restaurant's name begins with letter 'Wil'.")
db.restaurants.find({"$or": [
 {"cuisine": {$nin: ["American ", "Chinese"]}},
 {"name": /^Wil/}
]}, {_id: 0, restaurant_id: 1, name: 1, borough: 1, cuisine: 1}).forEach(printjson)

// Q23
print("\nQ23-Find the restaurant Id, name, and grades for those restaurants which achieved a grade of 'A' and scored 11 on an ISODate '2014-08-11T00:00:00Z' among many of survey dates.")
db.restaurants.find({"grades" : {$elemMatch: {"date": ISODate("2014-08-11T00:00:00Z"), "grade": "A", "score": 11}}}, {_id: 0, restaurant_id: 1, name: 1, grades: 1}).forEach(printjson)

// Q24
print("\nQ24-Find the restaurant Id, name and grades for those restaurants where the 2nd element of grades array contains a grade of 'A' and score 9 on an ISODate '2014-08-11T00:00:00Z'.")
db.restaurants.find({"$and": [
 {"grades.1.grade": "A"},
 {"grades.1.score": 9},
 {"grades.1.date": ISODate("2014-08-11T00:00:00Z")}
]}, {_id: 0, restaurant_id: 1, name: 1, grades: 1}).forEach(printjson)

// Q25
print("\nQ25-Find the restaurant Id, name, address and geographical location for those restaurants where 2nd element of coord array contains a value which is more than 42 and up to 52.")
db.restaurants.find({"$and": [
 {"address.coord.1": {"$gt": 42}},
 {"address.coord.1": {"$lte": 52}}
]}, {_id: 0, restaurant_id: 1, name: 1, address: 1}).forEach(printjson)

//----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

// Q3. Extra Credit: 40 points
print("\nQ3-Use the MongoDB geospatial facilities to find the nearest city to each meteorite 'fallen' (not found) since the year 1950, inclusive. Distance is between coordinates should be a straight line.")
print("\nNote: 'worldcities' is a CSV file. You will need to import into MongoDB AND clean-up the double quotes \nNote: Use the $near operator and select this closest entry per city.")

db.meteorites.createIndex({geolocation: "2dsphere"})   // This is to create a 2dsphere index on the geolocation field required for the $near
let citiesCursor = db.worldcities.find()
while (citiesCursor.hasNext()) {
   doc = citiesCursor.next()
   let longitude = doc.lng
   let latitude = doc.lat
    print("\ncity: "+doc.city+" => coordinates("+latitude+", "+longitude+")")  // (latitude, longitude)
   print("nearest meteorite info")
   // $near sorts the result by distance by default so the 1st entry will be the closest meteorite location
   // query foe only fallen meteorites and since year 1950
   db.meteorites.find({"$and": [
      {"fall": "Fell"},
       { "year": { $gte: "1950-01-01T00:00:00.000" } },    	//Doing simple string comparison will give the correct result  as all date is in yyyy-mm-dd format 
      {geolocation: {
          $near: { $geometry: { type: "Point", coordinates: [latitude,longitude]}}
      }}
   ]}).limit(1).forEach(printjson)
}

// ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
