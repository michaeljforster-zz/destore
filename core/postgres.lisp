;;;; core/postgres.lisp

;;; The MIT License (MIT)
;;;
;;; Copyright (c) 2016 Michael J. Forster
;;;
;;; Permission is hereby granted, free of charge, to any person obtaining a copy
;;; of this software and associated documentation files (the "Software"), to deal
;;; in the Software without restriction, including without limitation the rights
;;; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
;;; copies of the Software, and to permit persons to whom the Software is
;;; furnished to do so, subject to the following conditions:
;;;
;;; The above copyright notice and this permission notice shall be included in all
;;; copies or substantial portions of the Software.
;;;
;;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
;;; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
;;; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
;;; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
;;; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
;;; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
;;; SOFTWARE.

(defpackage "DESTORE/CORE/POSTGRES"
  (:use "CL")
  (:import-from "UUID")
  (:import-from "LOCAL-TIME")
  (:import-from "POSTMODERN")
  (:import-from "POSTMODERNITY")
  (:export "CREATE-DSTREAM"
           "DSTREAM"
           "DSTREAM-UUID"
           "DSTREAM-VERSION"
           "DSTREAM-TYPE"
           "DSTREAM-TYPE-KEY"
           "LIST-ALL-DSTREAMS"
           "WRITE-DEVENT"
           "DEVENT"
           "DEVENT-UUID"
           "DEVENT-TYPE"
           "DEVENT-METADATA"
           "DEVENT-PAYLOAD"
           "DEVENT-DSTREAM-UUID"
           "DEVENT-VERSION"
           "DEVENT-STORED-WHEN"
           "DEVENT-SEQUENCE-NO"
           "LIST-ALL-DEVENTS"
           "READ-DEVENTS"
           "DSNAPSHOT"
           "DSNAPSHOT-DSTREAM-UUID"
           "DSNAPSHOT-VERSION"
           "DSNAPSHOT-PAYLOAD"
           "DSNAPSHOT-STORED-WHEN"
           "WRITE-DSNAPSHOT"
           "READ-LAST-DSNAPSHOT"))

(in-package "DESTORE/CORE/POSTGRES")

(eval-when (:compile-toplevel :load-toplevel :execute)
  (local-time:set-local-time-cl-postgres-readers))

(defconstant +uuid-oid+ 2950)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (cl-postgres:set-sql-reader +uuid-oid+
                              #'uuid:make-uuid-from-string))

(defmethod cl-postgres:to-sql-string ((arg uuid:uuid))
  (princ-to-string arg))

(defun as-db-null (x)
  (if (null x)
      :null
      x))

(postmodernity:defpgstruct dstream
  uuid
  type
  type-key
  version)

(defun create-dstream (dstream-uuid dstream-type)
  (postmodern:query (:select 'dstream-uuid
                             'dstream-type
                             'dstream-type-key
                             'version
                     :from (:destore.create-dstream '$1 '$2))
                    dstream-uuid
                    dstream-type
                    :dstream))

(postmodern:defprepared list-all-dstreams
    (:select 'dstream-uuid
             'dstream-type
             'dstream-type-key
             'version
             :from (:destore.list-all-dstreams))
  :dstreams)

(defun write-devent (dstream-uuid
                     dstream-type-key
                     expected-version
                     devent-uuid
                     devent-type
                     metadata
                     payload)
  (postmodern:execute "BEGIN ISOLATION LEVEL SERIALIZABLE")
  (let ((transaction-finished-p nil))
    (unwind-protect
         (prog1
             (postmodern:query
              (:select (:destore.write-devent
                        '$1 '$2 '$3 '$4 '$5 '$6 '$7))
              dstream-uuid
              (as-db-null dstream-type-key)
              expected-version
              devent-uuid
              devent-type
              metadata
              payload
              :single)
           (postmodern:execute "COMMIT")
           (setf transaction-finished-p t))
      (unless transaction-finished-p
        (postmodern:execute "ROLLBACK")))))

(postmodernity:defpgstruct devent
  uuid
  type
  metadata
  payload
  dstream-uuid
  version
  stored-when
  sequence-no)

(postmodern:defprepared list-all-devents
    (:select 'devent-uuid
             'devent-type
             'metadata
             'payload
             'dstream-uuid
             'version
             'stored-when
             'sequence-no
             :from (:destore.list-all-devents))
  :devents)

(postmodern:defprepared-with-names read-devents (dstream-uuid
                                                 start-version)
  ((:select 'devent-uuid
            'devent-type
            'metadata
            'payload
            'dstream-uuid
            'version
            'stored-when
            'sequence-no
            :from (:destore.read-devents '$1 '$2))
   dstream-uuid
   start-version)
  :devents)

(postmodernity:defpgstruct dsnapshot
  dstream-uuid
  version
  payload
  stored-when)

(postmodern:defprepared-with-names write-dsnapshot (dstream-uuid
                                                    version
                                                    payload)
  ((:select (:destore.write-dsnapshot '$1 '$2 '$3))
   dstream-uuid
   version
   payload)
  :none)

(postmodern:defprepared-with-names read-last-dsnapshot (dstream-uuid)
  ((:select 'dstream-uuid
            'version
            'payload
            'stored-when
            :from (:destore.read-last-dsnapshot '$1))
   dstream-uuid)
  :dsnapshot)
