const { json } = require('express');
var express = require('express');
var router = express.Router();
var Booking = require('../model/config').Booking
var Guest = require('../model/config.js').Guest
 

router.get("/getCSV", async function (req, res) {
  // retrieving the user with a query...
  data = req.query.data === "Booking" ? Booking : Guest;
  date = `^${req.query.date ? req.query.date : ''}.*`
  if (data === "Booking"){
    jsonData = (await data.find({created_at: { $regex: date }}, {_id: 0}))
  }
  else {
    jsonData = (await data.find({}, {_id: 0}))
  }
  let csvData = Object.keys(jsonData[0]._doc).join(",") + "\r\n";
  jsonData.forEach((row) => {
    csvData += Object.values(row._doc).join(",") + "\r\n";
  })
  res
    .set({
      "Content-Type": "text/csv",
      "Content-Disposition": `attachment; filename="users.csv"`,
    })
    .send(csvData)

});

module.exports = router;
