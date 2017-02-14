;;;; test/postgres.lisp

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

(defpackage "DESTORE/TEST/POSTGRES"
  (:import-from "UUID")
  (:use "CL"
        "LISP-UNIT"
        "DESTORE/CORE/POSTGRES"
        "DESTORE/TEST/SUPPORT"))

(in-package "DESTORE/TEST/POSTGRES")

(defparameter *dref-type* "com.example.dref.user")

(defparameter *dref-count* 1000)

(define-test empty-destore
    (purge-destore)
  (with-connection
    ;; drefs
    (assert-equal 0 (count-drefs))
    (assert-equal 0 (count-drefs-of-type *dref-type*))
    (assert-equal '() (select-drefs))
    (assert-equal '() (select-drefs-of-type *dref-type*))
    ;; devents
    (assert-equal 0 (count-devents))
    (assert-equal '() (select-devents))
    ;; dsnapshots
    (assert-equal 0 (count-dsnapshots))
    (assert-equal '() (select-dsnapshots))))

(define-test insert-dref-succeeds
    (purge-destore)
  (with-connection
      (let ((dref-uuids '()))
        (dotimes (i *dref-count*)
          (let ((dref-uuid (genuuid)))
            (insert-dref dref-uuid *dref-type*)
            (push dref-uuid dref-uuids)))
        (flet ((selected-inserted-p (row)
                 (let ((dref-uuid (first row)))
                   (member dref-uuid dref-uuids :test #'uuid:uuid=)))
               (make-inserted-selected-p (row-fn)
                 #'(lambda (dref-uuid)
                     (let ((row (funcall row-fn dref-uuid)))
                       (and (not (null row))
                            (uuid:uuid= (first row) dref-uuid)))))
               (make-zerop (function &rest args)
                 #'(lambda (dref-uuid)
                     (zerop (apply function dref-uuid args))))
               (make-emptyp (function &rest args)
                 #'(lambda (dref-uuid)
                     (null (apply function dref-uuid args)))))
          ;; drefs
          (assert-equal *dref-count* (count-drefs))
          (assert-equal *dref-count* (count-drefs-of-type *dref-type*))
          (assert-true (every #'selected-inserted-p (select-drefs)))
          (assert-true (every #'selected-inserted-p (select-drefs-of-type *dref-type*)))
          (assert-true (every (make-inserted-selected-p #'select-dref) dref-uuids))
          ;; devents
          (assert-equal 0 (count-devents))
          (assert-true (every (make-zerop #'count-devents-for-dref-starting 0)
                              dref-uuids))
          (assert-equal '() (select-devents))
          (assert-true (every (make-emptyp #'select-devents-for-dref-starting 0)
                              dref-uuids))
          ;; snapshots
          (assert-equal 0 (count-dsnapshots))
          (assert-true (every (make-zerop #'count-dsnapshots-for-dref)
                              dref-uuids))
          (assert-equal '() (select-dsnapshots))
          (assert-true (every (make-emptyp #'select-dsnapshots-for-dref)
                              dref-uuids))
          (assert-true (every (make-emptyp #'select-last-dsnapshot-for-dref)
                              dref-uuids))))))

(define-test insert-dref-fails-on-dref-type-check
  (purge-destore)
  (with-connection
    (let ((dref-uuid (genuuid)))
      (insert-dref dref-uuid *dref-type*)
      (assert-error 'cl-postgres-error:check-violation (insert-dref dref-uuid ""))
      ;; drefs
      (assert-equal 1 (count-drefs))
      (assert-equal 1 (count-drefs-of-type *dref-type*))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs))))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs-of-type *dref-type*))))
      (assert-equality #'uuid:uuid= dref-uuid (first (select-dref dref-uuid)))
      ;; devents
      (assert-equal 0 (count-devents))
      (assert-equal 0 (count-devents-for-dref-starting dref-uuid 0))
      (assert-equal '() (select-devents))
      (assert-equal '() (select-devents-for-dref-starting dref-uuid 0))
      ;; snapshots
      (assert-equal 0 (count-dsnapshots))
      (assert-equal 0 (count-dsnapshots-for-dref dref-uuid))
      (assert-equal '() (select-dsnapshots))
      (assert-equal '() (select-dsnapshots-for-dref dref-uuid))
      (assert-equal nil (select-last-dsnapshot-for-dref dref-uuid)))))

(define-test insert-dref-fails-on-duplicate
    (purge-destore)
  (with-connection
      (let ((dref-uuid (genuuid)))
        (insert-dref dref-uuid *dref-type*)
        (assert-error 'cl-postgres-error:unique-violation (insert-dref dref-uuid *dref-type*))
        ;; drefs
        (assert-equal 1 (count-drefs))
        (assert-equal 1 (count-drefs-of-type *dref-type*))
        (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs))))
        (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs-of-type *dref-type*))))
        (assert-equality #'uuid:uuid= dref-uuid (first (select-dref dref-uuid)))
        ;; devents
        (assert-equal 0 (count-devents))
        (assert-equal 0 (count-devents-for-dref-starting dref-uuid 0))
        (assert-equal '() (select-devents))
        (assert-equal '() (select-devents-for-dref-starting dref-uuid 0))
        ;; snapshots
        (assert-equal 0 (count-dsnapshots))
        (assert-equal 0 (count-dsnapshots-for-dref dref-uuid))
        (assert-equal '() (select-dsnapshots))
        (assert-equal '() (select-dsnapshots-for-dref dref-uuid))
        (assert-equal nil (select-last-dsnapshot-for-dref dref-uuid)))))

(define-test insert-devent-succeeds
    (purge-destore)
  (with-connection
      (let ((dref-uuid (genuuid))
            (devent-uuids (list (genuuid)
                                (genuuid)
                                (genuuid))))
        (insert-dref dref-uuid *dref-type*)
        ;; devents
        (assert-equal 1 (insert-devent-returning dref-uuid
                                                 0
                                                 "Fred Forster"
                                                 (first devent-uuids)
                                                 "com.example.devent.user-created"
                                                 "IP-ADDR: 192.168.0.100"
                                                 "Fred:Forster:Brandon:MB:Canada"))
        (assert-equal 2 (insert-devent-returning dref-uuid
                                                 1
                                                 nil
                                                 (second devent-uuids)
                                                 "com.example.devent.user-licensed-vehicle"
                                                 "IP-ADDR: 192.168.0.100"
                                                 "Harley Davidson:Electra Glide"))
        (assert-equal 3 (insert-devent-returning dref-uuid
                                                 2
                                                 nil
                                                 (third devent-uuids)
                                                 "com.example.devent.user-went-driving"
                                                 "IP-ADDR: 192.168.0.100"
                                                 "Sturgis, Wyoming, Montana"))
        ;; drefs
        (assert-equal 1 (count-drefs))
        (assert-equal 1 (count-drefs-of-type *dref-type*))
        (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs))))
        (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs-of-type *dref-type*))))
        (assert-equality #'uuid:uuid= dref-uuid (first (select-dref dref-uuid)))
        (assert-equal 3 (third (select-dref dref-uuid)))
        ;; devents
        (assert-equal 3 (count-devents))
        (assert-equal 3 (count-devents-for-dref-starting dref-uuid 0))
        (assert-equality #'uuid:uuid=
                         (first devent-uuids)
                         (first (first (select-devents))))
        (assert-equality #'uuid:uuid=
                         (second devent-uuids)
                         (first (second (select-devents))))
        (assert-equality #'uuid:uuid=
                         (third devent-uuids)
                         (first (third (select-devents))))
        (assert-equality #'uuid:uuid=
                         (first devent-uuids)
                         (first (first (select-devents-for-dref-starting dref-uuid 0))))
        (assert-equality #'uuid:uuid=
                         (second devent-uuids)
                         (first (second (select-devents-for-dref-starting dref-uuid 0))))
        (assert-equality #'uuid:uuid=
                         (third devent-uuids)
                         (first (third (select-devents-for-dref-starting dref-uuid 0))))
        ;; snapshots
        (assert-equal 0 (count-dsnapshots))
        (assert-equal 0 (count-dsnapshots-for-dref dref-uuid))
        (assert-equal '() (select-dsnapshots))
        (assert-equal '() (select-dsnapshots-for-dref dref-uuid))
        (assert-equal nil (select-last-dsnapshot-for-dref dref-uuid)))))

(define-test insert-devent-fails-on-nonexistent-dref-rolls-back
  (purge-destore)
  (with-connection
    (let ((dref-uuid (genuuid))
          (devent-uuids (list (genuuid)
                              (genuuid)
                              (genuuid))))
      (insert-dref dref-uuid *dref-type*)
      ;; devents
      (assert-equal 1 (insert-devent-returning dref-uuid
                                               0
                                               "Fred Forster"
                                               (first devent-uuids)
                                               "com.example.devent.user-created"
                                               "IP-ADDR: 192.168.0.100"
                                               "Fred:Forster:Brandon:MB:Canada"))
      (assert-error 'cl-postgres:database-error (insert-devent-returning (genuuid)
                                                                         1
                                                                         nil
                                                                         (second devent-uuids)
                                                                         "com.example.devent.user-licensed-vehicle"
                                                                         "IP-ADDR: 192.168.0.100"
                                                                         "Harley Davidson:Electra Glide"))
      (assert-equal 2 (insert-devent-returning dref-uuid
                                               1
                                               nil
                                               (third devent-uuids)
                                               "com.example.devent.user-went-driving"
                                               "IP-ADDR: 192.168.0.100"
                                               "Sturgis, Wyoming, Montana"))
      ;; drefs
      (assert-equal 1 (count-drefs))
      (assert-equal 1 (count-drefs-of-type *dref-type*))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs))))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs-of-type *dref-type*))))
      (assert-equality #'uuid:uuid= dref-uuid (first (select-dref dref-uuid)))
      (assert-equal 2 (third (select-dref dref-uuid)))
      ;; devents
      (assert-equal 2 (count-devents))
      (assert-equal 2 (count-devents-for-dref-starting dref-uuid 0))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (second (select-devents))))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents-for-dref-starting dref-uuid 0))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (second (select-devents-for-dref-starting dref-uuid 0))))
      ;; snapshots
      (assert-equal 0 (count-dsnapshots))
      (assert-equal 0 (count-dsnapshots-for-dref dref-uuid))
      (assert-equal '() (select-dsnapshots))
      (assert-equal '() (select-dsnapshots-for-dref dref-uuid))
      (assert-equal nil (select-last-dsnapshot-for-dref dref-uuid)))))

(define-test insert-devent-fails-on-expected-version-rolls-back
  (purge-destore)
  (with-connection
    (let ((dref-uuid (genuuid))
          (devent-uuids (list (genuuid)
                              (genuuid)
                              (genuuid))))
      (insert-dref dref-uuid *dref-type*)
      ;; devents
      (assert-equal 1 (insert-devent-returning dref-uuid
                                               0
                                               "Fred Forster"
                                               (first devent-uuids)
                                               "com.example.devent.user-created"
                                               "IP-ADDR: 192.168.0.100"
                                               "Fred:Forster:Brandon:MB:Canada"))
      (assert-error 'cl-postgres:database-error (insert-devent-returning dref-uuid
                                                                         0
                                                                         nil
                                                                         (second devent-uuids)
                                                                         "com.example.devent.user-licensed-vehicle"
                                                                         "IP-ADDR: 192.168.0.100"
                                                                         "Harley Davidson:Electra Glide"))
      (assert-equal 2 (insert-devent-returning dref-uuid
                                               1
                                               nil
                                               (third devent-uuids)
                                               "com.example.devent.user-went-driving"
                                               "IP-ADDR: 192.168.0.100"
                                               "Sturgis, Wyoming, Montana"))
      ;; drefs
      (assert-equal 1 (count-drefs))
      (assert-equal 1 (count-drefs-of-type *dref-type*))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs))))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs-of-type *dref-type*))))
      (assert-equality #'uuid:uuid= dref-uuid (first (select-dref dref-uuid)))
      (assert-equal 2 (third (select-dref dref-uuid)))
      ;; devents
      (assert-equal 2 (count-devents))
      (assert-equal 2 (count-devents-for-dref-starting dref-uuid 0))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (second (select-devents))))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents-for-dref-starting dref-uuid 0))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (second (select-devents-for-dref-starting dref-uuid 0))))
      ;; snapshots
      (assert-equal 0 (count-dsnapshots))
      (assert-equal 0 (count-dsnapshots-for-dref dref-uuid))
      (assert-equal '() (select-dsnapshots))
      (assert-equal '() (select-dsnapshots-for-dref dref-uuid))
      (assert-equal nil (select-last-dsnapshot-for-dref dref-uuid)))))

(define-test insert-devent-fails-on-duplicate-rolls-back
  (purge-destore)
  (with-connection
    (let ((dref-uuid (genuuid))
          (devent-uuids (list (genuuid)
                              (genuuid)
                              (genuuid))))
      (insert-dref dref-uuid *dref-type*)
      ;; devents
      (assert-equal 1 (insert-devent-returning dref-uuid
                                               0
                                               "Fred Forster"
                                               (first devent-uuids)
                                               "com.example.devent.user-created"
                                               "IP-ADDR: 192.168.0.100"
                                               "Fred:Forster:Brandon:MB:Canada"))
      (assert-error 'cl-postgres:database-error (insert-devent-returning dref-uuid
                                                                         1
                                                                         nil
                                                                         (first devent-uuids)
                                                                         "com.example.devent.user-licensed-vehicle"
                                                                         "IP-ADDR: 192.168.0.100"
                                                                         "Harley Davidson:Electra Glide"))
      (assert-equal 2 (insert-devent-returning dref-uuid
                                               1
                                               nil
                                               (third devent-uuids)
                                               "com.example.devent.user-went-driving"
                                               "IP-ADDR: 192.168.0.100"
                                               "Sturgis, Wyoming, Montana"))
      ;; drefs
      (assert-equal 1 (count-drefs))
      (assert-equal 1 (count-drefs-of-type *dref-type*))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs))))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs-of-type *dref-type*))))
      (assert-equality #'uuid:uuid= dref-uuid (first (select-dref dref-uuid)))
      (assert-equal 2 (third (select-dref dref-uuid)))
      ;; devents
      (assert-equal 2 (count-devents))
      (assert-equal 2 (count-devents-for-dref-starting dref-uuid 0))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (second (select-devents))))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents-for-dref-starting dref-uuid 0))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (second (select-devents-for-dref-starting dref-uuid 0))))
      ;; snapshots
      (assert-equal 0 (count-dsnapshots))
      (assert-equal 0 (count-dsnapshots-for-dref dref-uuid))
      (assert-equal '() (select-dsnapshots))
      (assert-equal '() (select-dsnapshots-for-dref dref-uuid))
      (assert-equal nil (select-last-dsnapshot-for-dref dref-uuid)))))

(define-test insert-devent-fails-on-secondary-key-rolls-back
  (purge-destore)
  (with-connection
    (let ((dref-uuids (list (genuuid)
                            (genuuid)))
          (devent-uuids (list (genuuid)
                              (genuuid)
                              (genuuid))))
      (insert-dref (first dref-uuids) *dref-type*)
      (insert-dref (second dref-uuids) *dref-type*)
      ;; devents
      (assert-equal 1 (insert-devent-returning (first dref-uuids)
                                               0
                                               "Fred Forster"
                                               (first devent-uuids)
                                               "com.example.devent.user-created"
                                               "IP-ADDR: 192.168.0.100"
                                               "Fred:Forster:Brandon:MB:Canada"))
      (assert-equal 2 (insert-devent-returning (first dref-uuids)
                                               1
                                               nil
                                               (second devent-uuids)
                                               "com.example.devent.user-licensed-vehicle"
                                               "IP-ADDR: 192.168.0.100"
                                               "Harley Davidson:Electra Glide"))
      (assert-error 'cl-postgres:database-error
                    (insert-devent-returning (second dref-uuids)
                                             0
                                             "Fred Forster"
                                             (genuuid)
                                             "com.example.devent.user-created"
                                             "IP-ADDR: 10.10.0.100"
                                             "Fred:Forster:Killarney:MB:Canada"))
      (assert-equal 3 (insert-devent-returning (first dref-uuids)
                                               2
                                               nil
                                               (third devent-uuids)
                                               "com.example.devent.user-went-driving"
                                               "IP-ADDR: 192.168.0.100"
                                               "Sturgis, Wyoming, Montana"))
      ;; drefs
      (assert-equal 2 (count-drefs))
      (assert-equal 2 (count-drefs-of-type *dref-type*))
      ;; TODO if we can ASSERT- within each loop (rather than EVERY),
      ;; we add the loop var as an expression that will be displayed
      ;; on failure
      ;; 
      ;; TODO does lisp-unit provide setup/teardown so that we could
      ;; define SELECTED-INSERTED-P globally and avoid the FUNCALL
      ;; workaround?
      (flet ((selected-inserted-p (row)
               (let ((dref-uuid (first row)))
                 (member dref-uuid dref-uuids :test #'uuid:uuid=))))
        (assert-true (every #'selected-inserted-p (select-drefs)))
        (assert-true (every #'selected-inserted-p (select-drefs-of-type *dref-type*)))
        (assert-true (funcall #'selected-inserted-p (select-dref (first dref-uuids))))
        ;; (assert-true (selected-inserted-p (select-dref (second dref-uuids))))
        )
      (assert-equal 3 (third (select-dref (first dref-uuids))))
      (assert-equal 0 (third (select-dref (second dref-uuids))))
      ;; devents
      (assert-equal 3 (count-devents))
      (assert-equal 3 (count-devents-for-dref-starting (first dref-uuids) 0))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents))))
      (assert-equality #'uuid:uuid=
                       (second devent-uuids)
                       (first (second (select-devents))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (third (select-devents))))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents-for-dref-starting (first dref-uuids) 0))))
      (assert-equality #'uuid:uuid=
                       (second devent-uuids)
                       (first (second (select-devents-for-dref-starting (first dref-uuids) 0))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (third (select-devents-for-dref-starting (first dref-uuids) 0))))
      ;; snapshots
      (assert-equal 0 (count-dsnapshots))
      (assert-equal 0 (count-dsnapshots-for-dref (first dref-uuids)))
      (assert-equal '() (select-dsnapshots))
      (assert-equal '() (select-dsnapshots-for-dref (first dref-uuids)))
      (assert-equal nil (select-last-dsnapshot-for-dref (first dref-uuids))))))

(define-test insert-dsnapshot-succeeds
  (purge-destore)
  (with-connection
    (let ((dref-uuid (genuuid))
          (devent-uuids (list (genuuid)
                              (genuuid)
                              (genuuid))))
      (insert-dref dref-uuid *dref-type*)
      (insert-devent-returning dref-uuid
                               0
                               "Fred Forster"
                               (first devent-uuids)
                               "com.example.devent.user-created"
                               "IP-ADDR: 192.168.0.100"
                               "Fred:Forster:Brandon:MB:Canada")
      (insert-devent-returning dref-uuid
                               1
                               nil
                               (second devent-uuids)
                               "com.example.devent.user-licensed-vehicle"
                               "IP-ADDR: 192.168.0.100"
                               "Harley Davidson:Electra Glide")
      (insert-dsnapshot dref-uuid
                        2
                        "Fred:Forster:Brandon:MB:Canada:Harley Davidson:Electra Glide")
      (insert-devent-returning dref-uuid
                               2
                               nil
                               (third devent-uuids)
                               "com.example.devent.user-went-driving"
                               "IP-ADDR: 192.168.0.100"
                               "Sturgis, Wyoming, Montana")
      (insert-dsnapshot dref-uuid
                        3
                        "Fred:Forster:Brandon:MB:Canada:Harley Davidson:Electra Glide:Sturgis, Wyoming, Montana")
      ;; drefs
      (assert-equal 1 (count-drefs))
      (assert-equal 1 (count-drefs-of-type *dref-type*))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs))))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs-of-type *dref-type*))))
      (assert-equality #'uuid:uuid= dref-uuid (first (select-dref dref-uuid)))
      ;; devents
      (assert-equal 3 (count-devents))
      (assert-equal 3 (count-devents-for-dref-starting dref-uuid 0))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents))))
      (assert-equality #'uuid:uuid=
                       (second devent-uuids)
                       (first (second (select-devents))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (third (select-devents))))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents-for-dref-starting dref-uuid 0))))
      (assert-equality #'uuid:uuid=
                       (second devent-uuids)
                       (first (second (select-devents-for-dref-starting dref-uuid 0))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (third (select-devents-for-dref-starting dref-uuid 0))))
      ;; snapshots
      (assert-equal 2 (count-dsnapshots))
      (assert-equal 2 (count-dsnapshots-for-dref dref-uuid))
      (assert-true (every #'(lambda (row) (uuid:uuid= dref-uuid (first row)))
                          (select-dsnapshots)))
      (assert-true (every #'(lambda (row) (uuid:uuid= dref-uuid (first row)))
                          (select-dsnapshots-for-dref dref-uuid)))
      (assert-equality #'uuid:uuid= dref-uuid (first (select-last-dsnapshot-for-dref dref-uuid)))
      (assert-equal 3 (second (select-last-dsnapshot-for-dref dref-uuid))))))

(define-test insert-dsnapshot-fails-on-foreign-key-violation
  (purge-destore)
  (with-connection
    (let ((dref-uuid (genuuid))
          (devent-uuids (list (genuuid)
                              (genuuid)
                              (genuuid))))
      (insert-dref dref-uuid *dref-type*)
      (insert-devent-returning dref-uuid
                               0
                               "Fred Forster"
                               (first devent-uuids)
                               "com.example.devent.user-created"
                               "IP-ADDR: 192.168.0.100"
                               "Fred:Forster:Brandon:MB:Canada")
      (insert-devent-returning dref-uuid
                               1
                               nil
                               (second devent-uuids)
                               "com.example.devent.user-licensed-vehicle"
                               "IP-ADDR: 192.168.0.100"
                               "Harley Davidson:Electra Glide")
      (insert-dsnapshot dref-uuid
                        2
                        "Fred:Forster:Brandon:MB:Canada:Harley Davidson:Electra Glide")
      (insert-devent-returning dref-uuid
                               2
                               nil
                               (third devent-uuids)
                               "com.example.devent.user-went-driving"
                               "IP-ADDR: 192.168.0.100"
                               "Sturgis, Wyoming, Montana")
      (assert-error 'cl-postgres-error:foreign-key-violation
                    (insert-dsnapshot (genuuid)
                                      3
                                      "Fred:Forster:Brandon:MB:Canada:Harley Davidson:Electra Glide:Sturgis, Wyoming, Montana"))
      ;; drefs
      (assert-equal 1 (count-drefs))
      (assert-equal 1 (count-drefs-of-type *dref-type*))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs))))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (select-drefs-of-type *dref-type*))))
      (assert-equality #'uuid:uuid= dref-uuid (first (select-dref dref-uuid)))
      ;; devents
      (assert-equal 3 (count-devents))
      (assert-equal 3 (count-devents-for-dref-starting dref-uuid 0))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents))))
      (assert-equality #'uuid:uuid=
                       (second devent-uuids)
                       (first (second (select-devents))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (third (select-devents))))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (select-devents-for-dref-starting dref-uuid 0))))
      (assert-equality #'uuid:uuid=
                       (second devent-uuids)
                       (first (second (select-devents-for-dref-starting dref-uuid 0))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (third (select-devents-for-dref-starting dref-uuid 0))))
      ;; snapshots
      (assert-equal 1 (count-dsnapshots))
      (assert-equal 1 (count-dsnapshots-for-dref dref-uuid))
      (assert-true (every #'(lambda (row) (uuid:uuid= dref-uuid (first row)))
                          (select-dsnapshots)))
      (assert-true (every #'(lambda (row) (uuid:uuid= dref-uuid (first row)))
                          (select-dsnapshots-for-dref dref-uuid)))
      (assert-equality #'uuid:uuid= dref-uuid (first (select-last-dsnapshot-for-dref dref-uuid)))
      (assert-equal 2 (second (select-last-dsnapshot-for-dref dref-uuid))))))
