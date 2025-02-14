use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crate::props;
use get_routes::handler;
use helixdb::node_matches;
use helixdb::{
    helix_engine::graph_core::traversal::TraversalBuilder,
    helix_engine::graph_core::traversal_steps::{
        RSourceTraversalSteps, RTraversalBuilderMethods, RTraversalSteps, TraversalMethods,
        TraversalSearchMethods, WSourceTraversalSteps, WTraversalBuilderMethods, WTraversalSteps,
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    protocol::count::Count,
    protocol::response::Response,
    protocol::traversal_value::TraversalValue,
    protocol::{filterable::Filterable, value::Value, return_values::ReturnValue},
};
use serde::de;
use sonic_rs::{Deserialize, Serialize};

/**
* QUERY CreateEvent(userID, eventName, description, startDateTime, endDateTime, status, price,
   currency, maxCapacitiy, rating, createdAt, lastModified) =>
   user <- V(userID)
   event <- AddV<Event>({
       EventName: eventName, Description: description,
       StartDateTime:startDateTime, EndDateTime: endDateTime, Status: status, Price: price,
       Currency: currency, MaxCapacitiy: maxCapacitiy, Rating: rating, CreatedAt: createdAt,
       LastModified: lastModified
   })
   AddE<CreatedEvent>()::From(user)::To(event)
   RETURN user::ID
*/

#[handler]
pub fn create_event(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(2);

    #[derive(Serialize, Deserialize)]
    struct CreateEventData {
        user_id: String,
        event_name: String,
        description: String,
        start_date_time: String,
        end_date_time: String,
        status: String,
        price: f64,
        currency: String,
        max_capacity: i32,
        rating: i32,
        created_at: String,
        last_modified: String,
    }

    let data: CreateEventData = sonic_rs::from_slice(&input.request.body).unwrap();
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);

    tr.v_from_id(&txn, &data.user_id);
    let host = tr.finish()?;
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_v(
        &mut txn,
        "Event",
        props! {
            "EventName" => data.event_name,
            "Description" => data.description,
            "StartDateTime" => data.start_date_time,
            "EndDateTime" => data.end_date_time,
            "Status" => data.status,
            "Price" => data.price,
            "Currency" => data.currency,
            "MaxCapacity" => data.max_capacity,
            "Rating" => data.rating,
            "CreatedAt" => data.created_at,
            "LastModified" => data.last_modified,
        },
        None,
    );

    let event = tr.finish()?;
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_e(
        &mut txn,
        "CreatedEvent",
        &host.get_id()?,
        &event.get_id()?,
        props! {},
    );

    tr.result(txn)?;

    return_vals.insert("event".to_string(), ReturnValue::TraversalValues(event));

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn cancel_event(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct CancelEventData {
        event_id: String,
    }

    let data: CancelEventData = sonic_rs::from_slice(&input.request.body).unwrap();

    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);

    tr.v_from_id(&txn, &data.event_id)
        .update_props(&mut txn, props! { "Status" => "Cancelled" });

    tr.finish()?;

    Ok(())
}

/**
* QUERY CreateVenueBooking(venueID, userID, eventID, startDateTime, endDateTime, price, currency,
   status, createdAt, lastModified) =>
   venueBooking <- AddV<VenueBooking>({
       StartDateTime: startDateTime, EndDateTime: endDateTime,
       Price: price, Currency: currency, Status: status, CreatedAt: createdAt,
       LastModified: lastModified
   })
   AddE<Booking>()::From(venueBooking)::To(::V(venueID))
   AddE<BookedVenue>()::From(::V(user))::To(venueBooking)
   AddE<HeldAt>()::From(::V(eventID))::To(venueBooking)
   RETURN venueBooking::{
       BookingID: ::ID,
       VenueID: {::Out<Booking>()::ID},
       VenueName: {::Out<Booking>()::Props(VenueName)}
   }
*/

#[handler]
pub fn create_venue_booking(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct CreateVenueBookingData {
        venue_id: String,
        user_id: String,
        event_id: String,
        start_date_time: String,
        end_date_time: String,
        price: f64,
        currency: String,
        status: String,
        created_at: String,
        last_modified: String,
    }

    let data: CreateVenueBookingData = sonic_rs::from_slice(&input.request.body).unwrap();

    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);

    tr.add_v(
        &mut txn,
        "VenueBooking",
        props! {
            "StartDateTime" => data.start_date_time,
            "EndDateTime" => data.end_date_time,
            "Price" => data.price,
            "Currency" => data.currency,
            "Status" => data.status,
            "CreatedAt" => data.created_at,
            "LastModified" => data.last_modified,
        },
        None,
    );

    let venue_booking = tr.finish()?;
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);

    tr.add_e(
        &mut txn,
        "Booking",
        &venue_booking.get_id()?,
        &data.venue_id,
        props! {},
    );

    tr.add_e(
        &mut txn,
        "BookedVenue",
        &data.user_id,
        &venue_booking.get_id()?,
        props! {},
    );

    tr.add_e(
        &mut txn,
        "HeldAt",
        &data.event_id,
        &venue_booking.get_id()?,
        props! {},
    );

    tr.result(txn)?;
    Ok(())
}

#[handler]
pub fn cancel_venue_booking(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct CancelVenueBookingData {
        venue_booking_id: String,
    }

    let data: CancelVenueBookingData = sonic_rs::from_slice(&input.request.body).unwrap();

    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);

    tr.v_from_id(&txn, &data.venue_booking_id)
        .update_props(&mut txn, props! { "Status" => "Cancelled" });
    tr.in_e(&txn, "HeldAt").drop(&mut txn);

    tr.finish()?;

    response.body = sonic_rs::to_vec(&"Success").unwrap();

    Ok(())
}

/**
 * QUERY GetAllVenues(UserID, limit, lastPage) =>
    venues <- V<Venue>()::WHERE(_::Props(Status)::EQ("active"))::Range(limit, lastPage)  
    RETURN venues::{
        VenueInfo: {
            _::Props(SizeQM, Price, Currency, TimeUnit, MaxCapacitiy),
            Facilities: _::Out<HasFacility>,
            BookedDates: _::In<Booking>::Props(StartDateTime, EndDateTime)
        }
    }
 */

#[handler]
pub fn get_all_venues(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(2);

    #[derive(Serialize, Deserialize)]
    struct GetAllVenuesData {
        user_id: String,
        limit: usize,
        last_page: usize,
    }
    

    let data: GetAllVenuesData = sonic_rs::from_slice(&input.request.body).unwrap();

    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);

    tr.v_from_types(&txn, &["Venue".to_string()]).filter_nodes(&txn, node_matches!("Status", "active")).range(data.limit, data.last_page);
    let venues = tr.finish()?;

    struct Venue {
        
    }

    response.body = sonic_rs::to_vec(&"Success").unwrap();

    Ok(())
    
    
}