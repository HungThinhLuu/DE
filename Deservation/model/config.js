
var mongoose = require('mongoose');

mongoose.connect('mongodb+srv://hungthinhluu:hungthinhluu12345@cluster0.csitck1.mongodb.net/Reservation_System')
  .then(() => console.log("Connect MongoDB successfully!"))
  .catch(error => console.log(error))

exports.BookingSchema = new mongoose.Schema({
    arrive_at: String,
    booking_id: String,
    created_at: String,
    customer_id: String,
    id: String,
    pax: Number,
    status: String,
    store_id: String
});

exports.Booking = mongoose.model('Booking', exports.BookingSchema, 'Booking');

exports.guestSchema = new mongoose.Schema({
    birthday: String,
    email: String,
    first_name: String,
    gender: String,
    id: String,
    last_name: String,
    phone_number: String
});

exports.Guest = mongoose.model('Guest', exports.guestSchema, 'Guest');