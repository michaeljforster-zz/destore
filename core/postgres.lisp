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

;;; This module provides a thin wrapper around the PostgreSQL database
;;; that implements the destore. The objective here is to expose the
;;; DML and SQL as a Lispy substrate for testing and to implement the
;;; API.

(defpackage "DESTORE/CORE/POSTGRES"
  (:use "CL")
  (:import-from "UUID")
  (:import-from "LOCAL-TIME")
  (:import-from "POSTMODERN")
  (:import-from "POSTMODERNITY")
  (:export "GENUUID"
           "INSERT-DREF"
           "INSERT-DEVENT-RETURNING"
           "INSERT-DSNAPSHOT"
           "COUNT-DREFS"
           "COUNT-DREFS-OF-TYPE"
           "SELECT-DREFS"
           "SELECT-DREFS-OF-TYPE"
           "SELECT-DREF"
           "COUNT-DEVENTS"
           "COUNT-DEVENTS-FOR-DREF-STARTING"
           "SELECT-DEVENTS"
           "SELECT-DEVENTS-FOR-DREF-STARTING"
           "COUNT-DSNAPSHOTS"
           "COUNT-DSNAPSHOTS-FOR-DREF"
           "SELECT-DSNAPSHOTS"
           "SELECT-DSNAPSHOTS-FOR-DREF"
           "SELECT-LAST-DSNAPSHOT-FOR-DREF"))

(in-package "DESTORE/CORE/POSTGRES")

(eval-when (:compile-toplevel :load-toplevel :execute)
  (local-time:set-local-time-cl-postgres-readers))

(defconstant +uuid-oid+ 2950)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (cl-postgres:set-sql-reader +uuid-oid+
                              #'uuid:make-uuid-from-string))

(defmethod cl-postgres:to-sql-string ((arg uuid:uuid))
  (princ-to-string arg))

(defun genuuid ()
  (uuid:make-v4-uuid))

(defun as-db-null (x)
  (if (null x)
      :null
      x))

(defun as-nil (x)
  (if (eql x :null)
      nil
      x))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defmacro with-serializable-isolation (&body body)
    `(progn
       (postmodern:execute "BEGIN ISOLATION LEVEL SERIALIZABLE")
       (let ((transaction-finished-p nil))
         (unwind-protect
              (prog1
                  ,@body
                (postmodern:execute "COMMIT")
                (setf transaction-finished-p t))
           (unless transaction-finished-p
             (postmodern:execute "ROLLBACK")))))))

(postmodern:defprepared-with-names %insert-dref (dref-uuid dref-type)
  ((:insert-into 'destore.dref :set
                 'dref-uuid '$1
                 'dref-type '$2
                 'version '$3
                 'secondary-key-value '$4)
   dref-uuid
   dref-type
   0
   (as-db-null nil))
  :none)

(defun insert-dref (dref-uuid dref-type)
  (with-serializable-isolation
      (%insert-dref dref-uuid dref-type)))

(postmodern:defprepared-with-names %insert-devent-returning (dref-uuid
                                                             expected-version
                                                             secondary-key-value
                                                             devent-uuid
                                                             devent-type
                                                             metadata
                                                             payload)
  ((:select (:destore.insert-devent-returning '$1 '$2 '$3 '$4 '$5 '$6 '$7))
   dref-uuid
   expected-version
   (as-db-null secondary-key-value)
   devent-uuid
   devent-type
   metadata
   payload)
  :single)

(defun insert-devent-returning (dref-uuid
                                expected-version
                                secondary-key-value
                                devent-uuid
                                devent-type
                                metadata
                                payload)
  (with-serializable-isolation
      (%insert-devent-returning dref-uuid
                                expected-version
                                secondary-key-value
                                devent-uuid
                                devent-type
                                metadata
                                payload)))

(postmodern:defprepared-with-names %insert-dsnapshot (dref-uuid version payload)
  ((:select (:destore.insert-dsnapshot '$1 '$2 '$3))
   dref-uuid
   version
   payload)
  :none)

(defun insert-dsnapshot (dref-uuid version payload)
  (with-serializable-isolation
      (%insert-dsnapshot dref-uuid version payload)))

(defun row-elements-as-nil (row)
  (mapcar #'as-nil row))

(postmodern:defprepared count-drefs
    (:select (:count '*) :from 'destore.dref)
  :single)

(postmodern:defprepared-with-names count-drefs-of-type (dref-type)
  ((:select (:count '*) :from 'destore.dref :where (:= 'dref-type '$1))
   dref-type)
  :single)

(postmodern:defprepared %select-drefs
    (:select 'dref-uuid
             'dref-type
             'version
             'secondary-key-value
             :from 'destore.dref)
  :rows)

(defun select-drefs ()
  (mapcar #'row-elements-as-nil (%select-drefs)))

(postmodern:defprepared-with-names %select-drefs-of-type (dref-type)
  ((:select 'dref-uuid
            'dref-type
            'version
            'secondary-key-value
            :from 'destore.dref
            :where (:= 'dref-type '$1))
   dref-type)
  :rows)

(defun select-drefs-of-type (dref-uuid)
  (mapcar #'row-elements-as-nil (%select-drefs-of-type dref-uuid)))

(postmodern:defprepared-with-names %select-dref (dref-uuid)
  ((:select 'dref-uuid
            'dref-type
            'version
            'secondary-key-value
            :from 'destore.dref
            :where (:= 'dref-uuid '$1))
   dref-uuid)
  :row)

(defun select-dref (dref-uuid)
  (row-elements-as-nil (%select-dref dref-uuid)))

(postmodern:defprepared count-devents
    (:select (:count '*) :from 'destore.devent)
  :single)

(postmodern:defprepared-with-names count-devents-for-dref-starting (dref-uuid version)
  ((:select (:count '*)
            :from 'destore.devent
            :where (:and (:= 'dref-uuid '$1)
                         (:>= 'version '$2)))
   dref-uuid
   version)
  :single)

(postmodern:defprepared select-devents
    (:order-by
     (:select 'devent-uuid
              'devent-type
              'metadata
              'payload
              'dref-uuid
              'version
              'stored-when
              'sequence-no
              :from 'destore.devent)
     'sequence-no)
  :rows)

(postmodern:defprepared-with-names select-devents-for-dref-starting (dref-uuid version)
  ((:order-by
    (:select 'devent-uuid
             'devent-type
             'metadata
             'payload
             'dref-uuid
             'version
             'stored-when
             'sequence-no
             :from 'destore.devent
             :where (:and (:= 'dref-uuid '$1)
                          (:>= 'version '$2)))
    'sequence-no)
   dref-uuid
   version)
  :rows)

(postmodern:defprepared count-dsnapshots
    (:select (:count '*) :from 'destore.dsnapshot)
  :single)

(postmodern:defprepared-with-names count-dsnapshots-for-dref (dref-uuid)
  ((:select (:count '*) :from 'destore.dsnapshot :where (:= 'dref-uuid '$1))
   dref-uuid)
  :single)

(postmodern:defprepared select-dsnapshots
    (:select 'dref-uuid 'version 'payload 'stored-when :from 'destore.dsnapshot)
  :rows)

(postmodern:defprepared-with-names select-dsnapshots-for-dref (dref-uuid)
  ((:select 'dref-uuid 'version 'payload 'stored-when
            :from 'destore.dsnapshot
            :where (:= 'dref-uuid '$1))
   dref-uuid)
  :rows)

(postmodern:defprepared-with-names select-last-dsnapshot-for-dref (dref-uuid)
  ((:limit
    (:order-by
     (:select 'dref-uuid 'version 'payload 'stored-when
              :from 'destore.dsnapshot
              :where (:= 'dref-uuid '$1))
     (:desc 'version))
    1)
   dref-uuid)
  :row)
