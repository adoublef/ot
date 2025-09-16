create schema ot;

create table ot.device (
    id uuid primary key
    , metadata json -- { ..., lastSeen }
);