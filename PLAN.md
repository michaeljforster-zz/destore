# PLAN #####################################################################


## TODO #######################################################################


- README.md, including notes:

  - notes to initialize a DS within a database (create.sql)

  - note about test up:
  
        CREATE DATABASE destore_test;
        \c destore_test;
        \i sql/create.sql
        SET SEARCH_PATH = destore, "$user", public;


- docstrings in api.lisp




**CONDITIONS**

Goal is to wrap any PG errors with DESTORE specific conditions...


- CREATE-DREF
  - TODO check-type
  - insert-dref
    - CL-POSTGRES-ERROR:CHECK-VIOLATION on "dref_dref_type_check"
    - CL-POSTGRES-ERROR:UNIQUE-VIOLATION (dref-type secondary-key-value)
    - CL-POSTGRES:DATABASE-ERROR on "serialization failures, SQLSTATE value of '40001'"
  

- RECORD-DEVENT
  - TODO check-type
  - insert-devent-returning
    - CL-POSTGRES-ERROR:CHECK-VIOLATION on "devent_devent_type_check"
    - CL-POSTGRES:DATABASE-ERROR on "Nonexistent dref: UUID = " (in lieu of a FOREIGN-KEY-VIOLATION)
    - CL-POSTGRES:DATABASE-ERROR on "P0001: Concurrency problem: latest_version 1; expected_version 0"
    - CL-POSTGRES:DATABASE-ERROR on "serialization failures, SQLSTATE value of '40001'"
  

- SNAPSHOT
  - TODO check-type
  - select-dsnapshots
    - dref-uuid or lastp if provided
  - reduce-devents
    - select-devents
  - insert-dsnapshot
    - CL-POSTGRES-ERROR:FOREIGN-KEY-VIOLATION on "dsnapshot_dref_uuid_fkey"
    - CL-POSTGRES-ERROR:UNIQUE-VIOLATION (dref-uuid version)
    - CL-POSTGRES:DATABASE-ERROR on "serialization failures, SQLSTATE value of '40001'"



- FIND-DREF
  - select-dref
    - bad dref-uuid

- LIST-DREFS
  - select-drefs
    - bad dref-type if provided  

- COUNT-DREFS

- COUNT-DEVENTS
  - count-devents - bad dref-uuid or version if provided

- COUNT-DSNAPSHOTS
  - count-dsnapshots - dref-uuid if provided



- RECONSTITUTE
  - TODO check-type
  - select-dsnapshots
    - dref-uuid or lastp if provided
  - reduce-devents
    - select-devents
      - bad dref-uuid or version if provided


- PROJECT
  - TODO check-type
  - reduce-devents
    - select-devents
      - bad dref-uuid or version if provided







TODO test API - review scratch.lisp examples

-------------------------------------------------------------------------------

DONE higher level API
DONE Simplify PostgreSQL layer
DONE Hide details of dstream/devent version management
DONE project
DONE wrap create-dstream in serializable transaction
DONE wrap write-dsnapshot in serializable transaction
DONE switch to plain TEXT
DONE reconstitute
DONE core/postgres - low level PG bindings
DONE test/postgres to test those bindings



## DESIGN #####################################################################

### Database

    CREATE DATABASE destore_dev;
    \c destore_dev
    \i sql/create.sql
    SET SEARCH_PATH = destore, "$user", public;

### PostgreSQL Transaction Isolation

NOTE: We will be reading and updating the stream version. We must use
"Serializable" transaction isolation level in PG; even "Repeatable
Read" is unsuitable.

    # SHOW DEFAULT_TRANSACTION_ISOLATION;

     default_transaction_isolation 
    -------------------------------
     read committed
    (1 row)


    # BEGIN ISOLATION LEVEL SERIALIZABLE;
      SELECT destore.append_stream_event ('6ae72589-a908-46ec-b12d-83197c669e4c'::uuid, 'ca.authentigate.test.event', 8, '03dd145c-d021-4753-b1fb-a78387811c5c'::uuid, 'ca.authentigate.test.event-renamed', '{}',
      '{"event-id": "6ae72589-a908-46ec-b12d-83197c669e4c", "event-name": "B0RK3D 3000"}', NULL);
      COMMIT;

See test.sql for an expanded example.


file:///opt/local/share/doc/postgresql95/html/transaction-iso.html

"13.2.2 Repeatable Read Isolation Level

...

Attempts to enforce business rules by transactions running at this
isolation level are not likely to work correctly without careful use
of explicit locks to block conflicting transactions."

"13.2.3 Serializable Isolation Level

... applications using this level must be prepared to retry
transactions due to serialization failures.

...

ERROR:  could not serialize access due to read/write dependencies among transactions

...

When relying on Serializable transactions to prevent anomalies, it is
important that any data read from a permanent user table not be
considered valid until the transaction which read it has successfully
committed.

...

Consistent use of Serializable transactions can simplify development.

...

It is important that an environment which uses this technique have a
generalized way of handling serialization failures (which always
return with a SQLSTATE value of '40001'), ...

...

For optimal performance when relying on Serializable transactions for
concurrency control, these issues should be considered:

    Declare transactions as READ ONLY when possible.

    Control the number of active connections, using a connection pool
    if needed. This is always an important performance consideration,
    but it can be particularly important in a busy system using
    Serializable transactions.

    Don't put more into a single transaction than needed for integrity
    purposes.

    Don't leave connections dangling "idle in transaction" longer than
    necessary.

    Eliminate explicit locks, SELECT FOR UPDATE, and SELECT FOR SHARE
    where no longer needed due to the protections automatically
    provided by Serializable transactions.

    When the system is forced to combine multiple page-level predicate
    locks into a single relation-level predicate lock because the
    predicate lock table is short of memory, an increase in the rate
    of serialization failures may occur. You can avoid this by
    increasing max_pred_locks_per_transaction.

    A sequential scan will always necessitate a relation-level
    predicate lock. This can result in an increased rate of
    serialization failures. It may be helpful to encourage the use of
    index scans by reducing random_page_cost and/or increasing
    cpu_tuple_cost. Be sure to weigh any decrease in transaction
    rollbacks and restarts against any overall change in query
    execution time.

"
