-- create.sql

-- The MIT License (MIT)
--
-- Copyright (c) 2016 Michael J. Forster
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in all
-- copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
-- SOFTWARE.

-- Isolate the devent store in its own schema to maintain a clear
-- boundary with other database uses.
-- HINT: set search_path to destore;

CREATE SCHEMA destore;

-- Every aggregate in the domain should be represented by a dref and
-- associated devents.  The dref and its type are recorded here. The
-- dref version must match that of the latest devent associated with
-- the dref (see destore.devent.version); the version serves as an
-- optimistic concurrency check (see
-- destore.insert_devent_returning()).  Together, dref_type and
-- secondary_key_value support an optional secondary key for the
-- aggregate (see destore.insert_devent_returning()).

CREATE TABLE destore.dref
( dref_uuid uuid PRIMARY KEY
, dref_type varchar(255) NOT NULL CHECK (trim(dref_type) <> '')
, version integer NOT NULL
, secondary_key_value varchar(255) -- can be null
, UNIQUE(dref_type, secondary_key_value)
);

-- Each devent stored has an incremented version number, which is
-- unique and sequential only within the context of the given dref
-- (see destore.dref.version).

CREATE TABLE destore.devent
( devent_uuid uuid PRIMARY KEY
, devent_type varchar(255) NOT NULL CHECK (trim(devent_type) <> '')
, metadata text NOT NULL
, payload text NOT NULL
, dref_uuid uuid NOT NULL REFERENCES destore.dref ON UPDATE RESTRICT
, version integer NOT NULL
, stored_when timestamp with time zone NOT NULL
, sequence_no bigserial NOT NULL
, UNIQUE (dref_uuid, version)
);

-- The operation for writing an devent to a dref. In adition to the
-- dref's UUID and type, the caller must specify what is expected
-- to be the current version of the dref--and of the latest stored
-- devent for the dref. This constitutes an optimistic concurrency
-- check, and the versions of the writeed devent will increment from
-- the version.
-- 
-- Note that this operation must be performed within a serializable
-- transaction.

CREATE OR REPLACE FUNCTION destore.insert_devent_returning
( the_dref_uuid uuid
, expected_version integer
, the_secondary_key_value varchar
, the_devent_uuid uuid
, the_devent_type varchar
, the_metadata text
, the_payload text
)
RETURNS integer AS $$
DECLARE
  latest_version integer;
BEGIN
  latest_version = version FROM destore.dref
                   WHERE dref_uuid = the_dref_uuid;

  -- Be more explicit than relying upon the NOT NULL check.
  IF latest_version IS NULL THEN
    RAISE EXCEPTION 'Nonexistent dref: UUID = %', the_dref_uuid;
  END IF;

  IF expected_version != latest_version THEN
     RAISE EXCEPTION
           'Concurrency problem: latest_version = %; expected_version = %',
           latest_version, expected_version;
  END IF;

  latest_version = latest_version + 1;

  INSERT INTO destore.devent
  ( devent_uuid
  , devent_type
  , metadata
  , payload
  , dref_uuid
  , version
  , stored_when)
  VALUES
  ( the_devent_uuid
  , the_devent_type
  , the_metadata
  , the_payload
  , the_dref_uuid
  , latest_version
  , current_timestamp);

  IF the_secondary_key_value IS NOT NULL THEN
     UPDATE destore.dref
     SET secondary_key_value = the_secondary_key_value
     WHERE dref_uuid = the_dref_uuid;
  END IF;

  UPDATE destore.dref
  SET version = latest_version
  WHERE dref_uuid = the_dref_uuid;

  RETURN latest_version;
END;
$$ LANGUAGE plpgsql;

-- An aggregate can be reconstituted by replaying its devent history,
-- but that will become more expensive as the number of devents
-- grows. By periodically reconstituting an aggregate up to the latest
-- devent and storing a versioned snapshot of the aggregate--a
-- dsnapshot--subsequent reconstitutions need only begin with later
-- devents, using the dsnapshot as a starting point.
-- 
-- The dsnapshot version must match that of the latest devent used to
-- create the dref's dsnapshot (see destore.dref.version). Betware
-- that this is not enforced by the database design.

CREATE TABLE destore.dsnapshot
( dref_uuid uuid NOT NULL
  REFERENCES destore.dref ON UPDATE RESTRICT
, version integer NOT NULL
, payload text NOT NULL
-- This is meta-data for debugging and other maintenance purposes.
, stored_when timestamp with time zone NOT NULL
, PRIMARY KEY (dref_uuid, version)
);

CREATE OR REPLACE FUNCTION destore.insert_dsnapshot
( the_dref_uuid uuid
, the_version integer
, the_payload text)
RETURNS void AS $$
DECLARE
  exists_dref boolean;
BEGIN
  INSERT INTO destore.dsnapshot
  (dref_uuid, version, payload, stored_when)
  VALUES
  (the_dref_uuid, the_version, the_payload, current_timestamp);
END;
$$ LANGUAGE plpgsql;
