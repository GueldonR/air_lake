# Source schema

The generated sample data imitates extracts from an airline's operational system.
These are the source tables as they land in bronze, before any modelling.

```mermaid
erDiagram
    airlines ||--o{ aircraft : "owns"
    airlines ||--o{ flight_schedules : "operates"
    airports ||--o{ flight_schedules : "origin"
    airports ||--o{ flight_schedules : "destination"
    flight_schedules ||--o{ flights : "generates"
    aircraft ||--o{ flights : "flies"
    passengers ||--o{ bookings : "makes"
    bookings ||--|{ booking_segments : "contains"
    flights ||--o{ booking_segments : "booked on"
    bookings ||--|{ payments : "paid by"

    airlines {
        string airline_id PK
        string iata_code
        string airline_name
        string country
        timestamp created_at
        timestamp updated_at
    }
    airports {
        string airport_id PK
        string iata_code
        string airport_name
        string city
        string country
        decimal latitude
        decimal longitude
        timestamp created_at
        timestamp updated_at
    }
    aircraft {
        string aircraft_id PK
        string registration
        string airline_id FK
        string model
        int seat_capacity
        string status "active | maintenance"
        timestamp created_at
        timestamp updated_at
    }
    flight_schedules {
        string schedule_id PK
        string airline_id FK
        string flight_number
        string origin_airport_id FK
        string destination_airport_id FK
        string departure_time "HH:MM"
        int duration_minutes
        string days_of_week "e.g. 1357"
        date valid_from
        date valid_to
        timestamp created_at
        timestamp updated_at
    }
    flights {
        string flight_id PK
        string schedule_id FK
        string flight_number
        string aircraft_id FK
        date flight_date
        timestamp scheduled_departure
        timestamp scheduled_arrival
        timestamp estimated_departure
        timestamp actual_departure
        timestamp actual_arrival
        string status "scheduled | delayed | departed | arrived | cancelled"
        timestamp created_at
        timestamp updated_at
    }
    passengers {
        string passenger_id PK
        string first_name
        string last_name
        string email
        string phone
        date date_of_birth
        string nationality
        timestamp created_at
        timestamp updated_at
    }
    bookings {
        string booking_id PK
        string booking_reference "6-char PNR"
        string passenger_id FK
        timestamp booked_at
        string channel "web | app | agent"
        string currency
        decimal total_amount
        string status "confirmed | cancelled | completed"
        timestamp created_at
        timestamp updated_at
    }
    booking_segments {
        string segment_id PK
        string booking_id FK
        int segment_no
        string flight_id FK
        string cabin_class "economy | premium_economy | business"
        decimal fare_amount
        timestamp created_at
    }
    payments {
        string payment_id PK
        string booking_id FK
        string payment_type "charge | refund"
        string method "card | paypal | klarna | invoice"
        decimal amount
        string currency
        string status "captured | failed | refunded"
        timestamp created_at
    }
```

## Two kinds of tables

| Kind | Tables | How rows change |
|---|---|---|
| Mutable entities | airlines, airports, aircraft, flight_schedules, flights, passengers, bookings | Updated in place; `updated_at` says which version is newest |
| Insert-only events | booking_segments, payments | Never updated; a refund is a new payment row |

## Batches

| Batch | Contents |
|---|---|
| `initial` | Current state of every row as of 2026-06-01 |
| `incremental` | Every version of every row changed 2026-06-01 → 2026-07-01, unordered |
| `scd` | Every version changed 2026-07-01 → 2026-08-01, plus some older versions replayed late |
| `dirty` | Rows that should fail quality checks: null keys, orphan FKs, bad values, duplicates |
