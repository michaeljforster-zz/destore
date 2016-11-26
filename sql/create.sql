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

-- Every aggregate in the domain should be represented by a dstream of
-- devents.  The dstream and its type are recorded here. The dstream
-- version must match that of the latest devent for the dstream (see
-- destore.devent.version); the version serves as an optimistic
-- concurrency check (see destore.append_devent()).  Together,
-- dstream_type and dstream_type_key support an optional secondary key
-- for the aggregate (see destore.append_devent()).

CREATE TABLE destore.dstream
( dstream_uuid uuid PRIMARY KEY
, version integer NOT NULL
, dstream_type varchar(255) NOT NULL CHECK (trim(dstream_type) <> '')
, dstream_type_key varchar(255) -- can be null
, UNIQUE(dstream_type, dstream_type_key)
);

CREATE OR REPLACE FUNCTION destore.list_all_dstreams ()
RETURNS SETOF destore.dstream AS $$
  SELECT
    dstream_uuid
  , version
  , dstream_type
  , dstream_type_key
  FROM destore.dstream;
$$ LANGUAGE sql;

-- Each devent stored has an incremented version number, which
-- is unique and sequential only within the context of the given
-- dstream (see destore.dstream.version).

CREATE TABLE destore.devent
( devent_uuid uuid PRIMARY KEY
, devent_type varchar(255) NOT NULL CHECK (trim(devent_type) <> '')
, metadata json NOT NULL -- jsonb not available in 9.3
, payload json NOT NULL -- jsonb not available in 9.3
, dstream_uuid uuid NOT NULL REFERENCES destore.dstream ON UPDATE RESTRICT
, version integer NOT NULL
, stored_when timestamp with time zone NOT NULL
, sequence_no bigserial NOT NULL
, UNIQUE (dstream_uuid, version)
);

-- The operation for appending an devent to a dstream. In adition to
-- the dstream's UUID and type, the caller must specify what is
-- expected to be the current version of the dstream--and of the latest
-- stored devent for the dstream. This constitutes an optimistic
-- concurrency check, and the versions of the appended devent will
-- increment from the version.
-- 
-- Note that this operation must be performed within a serializable
-- transaction.

CREATE OR REPLACE FUNCTION destore.append_devent
( the_dstream_uuid uuid
, the_dstream_type varchar
, expected_version integer
, the_devent_uuid uuid
, the_devent_type varchar(255)
, the_metadata json -- jsonb not available in 9.3
, the_payload json -- jsonb not available in 9.3
, the_dstream_type_key varchar(255))
RETURNS integer AS $$
DECLARE
  existing_dstream_type varchar;
  latest_version integer;
BEGIN
  existing_dstream_type = dstream_type FROM destore.dstream
                   WHERE dstream_uuid = the_dstream_uuid;

  IF existing_dstream_type IS NOT NULL
  AND existing_dstream_type != the_dstream_type THEN
    RAISE EXCEPTION
          'Incorrect dstream_type: % does not match %',
          the_dstream_type, existing_dstream_type ;

  END IF;

  latest_version = version FROM destore.dstream
                   WHERE dstream_uuid = the_dstream_uuid;
                   
  IF latest_version IS NULL THEN
    latest_version = 0;
    INSERT INTO destore.dstream
    (dstream_uuid, version, dstream_type, dstream_type_key)
    VALUES
    (the_dstream_uuid, latest_version, the_dstream_type, null);
  END IF;

  IF expected_version != latest_version THEN
     RAISE EXCEPTION
           'Concurrency problem: latest_version %; expected_version %',
           latest_version, expected_version;
  END IF;

  latest_version = latest_version + 1;

  INSERT INTO destore.devent
  ( devent_uuid
  , devent_type
  , metadata
  , payload
  , dstream_uuid
  , version
  , stored_when)
  VALUES
  ( the_devent_uuid
  , the_devent_type
  , the_metadata
  , the_payload
  , the_dstream_uuid
  , latest_version
  , current_timestamp);

  UPDATE destore.dstream SET dstream_type_key = the_dstream_type_key
  WHERE dstream_uuid = the_dstream_uuid;

  UPDATE destore.dstream SET version = latest_version
  WHERE dstream_uuid = the_dstream_uuid;
  
  RETURN latest_version;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION destore.list_all_devents ()
RETURNS SETOF destore.devent AS $$
  SELECT
    devent_uuid
  , devent_type
  , metadata
  , payload
  , dstream_uuid
  , version
  , stored_when
  , sequence_no
  FROM destore.devent
  ORDER BY sequence_no;
$$ LANGUAGE sql;

-- The operation to read devents in a dstream from the specified version
-- forward.

CREATE OR REPLACE FUNCTION destore.read_devents
(the_dstream_uuid uuid, start_version integer)
RETURNS SETOF destore.devent AS $$
  SELECT
    devent_uuid
  , devent_type
  , metadata
  , payload
  , dstream_uuid
  , version
  , stored_when
  , sequence_no
  FROM destore.devent
  WHERE dstream_uuid = the_dstream_uuid AND version >= start_version
  ORDER BY version;
$$ LANGUAGE sql;

CREATE TABLE destore.dsnapshot
( dstream_uuid uuid NOT NULL
  REFERENCES destore.dstream ON UPDATE RESTRICT
-- This version number must match the version of the latest
-- domain_devent used to create the dstream's dsnapshot (see
-- destore.dstream.version). Betware that this is not enforced.
, version integer NOT NULL
, payload json NOT NULL -- jsonb not available in 9.3
-- This is meta-data for debugging and other maintenance purposes.
, stored_when timestamp with time zone NOT NULL
, PRIMARY KEY (dstream_uuid, version)
);

CREATE OR REPLACE FUNCTION destore.write_dsnapshot
( the_dstream_uuid uuid
, the_version integer
, the_payload json) -- jsbonb not available in 9.3
RETURNS void AS $$
BEGIN
  INSERT INTO destore.dsnapshot
  (dstream_uuid, version, payload, stored_when)
  VALUES
  (the_dstream_uuid, the_version, the_payload, current_timestamp);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION destore.read_last_dsnapshot
(the_dstream_uuid uuid)
RETURNS SETOF destore.dsnapshot AS $$
  SELECT dstream_uuid, version, payload, stored_when
  FROM destore.dsnapshot
  ORDER BY version DESC
  LIMIT 1
$$ LANGUAGE sql;
