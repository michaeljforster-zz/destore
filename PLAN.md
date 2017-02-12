# PLAN #####################################################################


## TODO #######################################################################


- README.md, including notes:

  - notes to initialize a DS within a database (create.sql)

  - note about test up:
  
        CREATE DATABASE destore_test;
        \c destore_test;
        \i sql/create.sql
        SET SEARCH_PATH = destore, "$user", public;





  - handling serialization failures, SQLSTATE value of '40001'

  - handling concurrency faiures, Database error P0001: Concurrency problem: latest_version 1; expected_version 0

  - WRITE-DEVENT can signal "Nonexistent dstream" or "Concurrency Problem" or "Unique Violation"




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
