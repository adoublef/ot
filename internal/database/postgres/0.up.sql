create schema iot;

create table iot.device (
    id uuid primary key
    , metadata json -- { ..., lastSeen }
);