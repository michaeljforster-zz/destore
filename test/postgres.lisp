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
    (assert-equal 0 (destore/core/postgres:count-drefs))
    (assert-equal 0 (destore/core/postgres:count-drefs-of-type *dref-type*))
    (assert-equal '() (destore/core/postgres:select-drefs))
    (assert-equal '() (destore/core/postgres:select-drefs-of-type *dref-type*))
    ;; (destore/core/postgres:select-dref ...)
    ;; devents
    (assert-equal 0 (destore/core/postgres:count-devents))
    ;; (destore/core/postgres:count-devents-for-dref-starting ...)
    (assert-equal '() (destore/core/postgres:select-devents))
    ;; (destore/core/postgres:select-devents-for-dref-starting ...)
    ;; dsnapshots
    (assert-equal 0 (destore/core/postgres:count-dsnapshots))
    ;; (destore/core/postgres:count-dsnapshots-for-dref ...)
    (assert-equal '() (destore/core/postgres:select-dsnapshots))
    ;; (destore/core/postgres:select-dsnapshots-for-dref ...)
    ;; (destore/core/postgres:select-last-dsnapshots-for-dref ...)
    ))

(define-test insert-drefs-successfully
  (purge-destore)
  (with-connection
    (let ((dref-uuids '()))
      (dotimes (i *dref-count*)
        (let ((dref-uuid (destore/core/postgres:genuuid)))
          (destore/core/postgres:insert-dref dref-uuid *dref-type*)
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
        (assert-equal *dref-count* (destore/core/postgres:count-drefs))
        (assert-equal *dref-count* (destore/core/postgres:count-drefs-of-type *dref-type*))
        (assert-true (every #'selected-inserted-p (destore/core/postgres:select-drefs)))
        (assert-true (every #'selected-inserted-p (destore/core/postgres:select-drefs-of-type *dref-type*)))
        (assert-true (every (make-inserted-selected-p #'destore/core/postgres:select-dref) dref-uuids))
        ;; devents
        (assert-equal 0 (destore/core/postgres:count-devents))
        (assert-true (every (make-zerop #'destore/core/postgres:count-devents-for-dref-starting 0)
                            dref-uuids))
        (assert-equal '() (destore/core/postgres:select-devents))
        (assert-true (every (make-emptyp #'destore/core/postgres:select-devents-for-dref-starting 0)
                            dref-uuids))
        ;; snapshots
        (assert-equal 0 (destore/core/postgres:count-dsnapshots))
        (assert-true (every (make-zerop #'destore/core/postgres:count-dsnapshots-for-dref)
                            dref-uuids))
        (assert-equal '() (destore/core/postgres:select-dsnapshots))
        (assert-true (every (make-emptyp #'destore/core/postgres:select-dsnapshots-for-dref)
                            dref-uuids))
        (assert-true (every (make-emptyp #'destore/core/postgres:select-last-dsnapshot-for-dref)
                            dref-uuids))))))

;; TODO INSERT-DREF w/ duplicate uuid raises error, not commited, and select-xxx return same results as before
;; note that here we're testing the raw PG conditions raised; we'll test the api level conditions elsewhere


(define-test insert-devents-successfully
  (purge-destore)
  (with-connection
    (let ((dref-uuid (destore/core/postgres:genuuid))
          (devent-uuids (list (destore/core/postgres:genuuid)
                              (destore/core/postgres:genuuid)
                              (destore/core/postgres:genuuid))))
      (destore/core/postgres:insert-dref dref-uuid *dref-type*)
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
      (assert-equal 1 (destore/core/postgres:count-drefs))
      (assert-equal 1 (destore/core/postgres:count-drefs-of-type *dref-type*))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (destore/core/postgres:select-drefs))))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (destore/core/postgres:select-drefs-of-type *dref-type*))))
      (assert-equality #'uuid:uuid= dref-uuid (first (destore/core/postgres:select-dref dref-uuid)))
      (assert-equal 3 (third (destore/core/postgres:select-dref dref-uuid)))
      ;; devents
      (assert-equal 3 (destore/core/postgres:count-devents))
      (assert-equal 3 (destore/core/postgres:count-devents-for-dref-starting dref-uuid 0))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (destore/core/postgres:select-devents))))
      (assert-equality #'uuid:uuid=
                       (second devent-uuids)
                       (first (second (destore/core/postgres:select-devents))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (third (destore/core/postgres:select-devents))))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (destore/core/postgres:select-devents-for-dref-starting dref-uuid 0))))
      (assert-equality #'uuid:uuid=
                       (second devent-uuids)
                       (first (second (destore/core/postgres:select-devents-for-dref-starting dref-uuid 0))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (third (destore/core/postgres:select-devents-for-dref-starting dref-uuid 0))))
      ;; snapshots
      (assert-equal 0 (destore/core/postgres:count-dsnapshots))
      (assert-equal 0 (destore/core/postgres:count-dsnapshots-for-dref dref-uuid))
      (assert-equal '() (destore/core/postgres:select-dsnapshots))
      (assert-equal '() (destore/core/postgres:select-dsnapshots-for-dref dref-uuid))
      (assert-equal nil (destore/core/postgres:select-last-dsnapshot-for-dref dref-uuid)))))


;; TODO (DESTORE/CORE/POSTGRES:INSERT-DEVENT-RETURNING ) and conditions...
;; TODO ... denonstrate various invariants, concurrency error, etc.

(define-test insert-dsnapshot-successfully
  (purge-destore)
  (with-connection
    (let ((dref-uuid (destore/core/postgres:genuuid))
          (devent-uuids (list (destore/core/postgres:genuuid)
                              (destore/core/postgres:genuuid)
                              (destore/core/postgres:genuuid))))
      (destore/core/postgres:insert-dref dref-uuid *dref-type*)
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
      (insert-devent-returning dref-uuid
                               2
                               nil
                               (third devent-uuids)
                               "com.example.devent.user-went-driving"
                               "IP-ADDR: 192.168.0.100"
                               "Sturgis, Wyoming, Montana")
      (destore/core/postgres:insert-dsnapshot dref-uuid
                                              3
                                              "Fred:Forster:Brandon:MB:Canada:Harley Davidson:Electra Glide:Sturgis, Wyoming, Montana")
      ;; drefs
      (assert-equal 1 (destore/core/postgres:count-drefs))
      (assert-equal 1 (destore/core/postgres:count-drefs-of-type *dref-type*))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (destore/core/postgres:select-drefs))))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (destore/core/postgres:select-drefs-of-type *dref-type*))))
      (assert-equality #'uuid:uuid= dref-uuid (first (destore/core/postgres:select-dref dref-uuid)))
      ;; devents
      (assert-equal 3 (destore/core/postgres:count-devents))
      (assert-equal 3 (destore/core/postgres:count-devents-for-dref-starting dref-uuid 0))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (destore/core/postgres:select-devents))))
      (assert-equality #'uuid:uuid=
                       (second devent-uuids)
                       (first (second (destore/core/postgres:select-devents))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (third (destore/core/postgres:select-devents))))
      (assert-equality #'uuid:uuid=
                       (first devent-uuids)
                       (first (first (destore/core/postgres:select-devents-for-dref-starting dref-uuid 0))))
      (assert-equality #'uuid:uuid=
                       (second devent-uuids)
                       (first (second (destore/core/postgres:select-devents-for-dref-starting dref-uuid 0))))
      (assert-equality #'uuid:uuid=
                       (third devent-uuids)
                       (first (third (destore/core/postgres:select-devents-for-dref-starting dref-uuid 0))))
      ;; snapshots
      (assert-equal 1 (destore/core/postgres:count-dsnapshots))
      (assert-equal 1 (destore/core/postgres:count-dsnapshots-for-dref dref-uuid))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (destore/core/postgres:select-dsnapshots))))
      (assert-equality #'uuid:uuid= dref-uuid (first (first (destore/core/postgres:select-dsnapshots-for-dref dref-uuid))))
      (assert-equality #'uuid:uuid= dref-uuid (first (destore/core/postgres:select-last-dsnapshot-for-dref dref-uuid))))))






;; TODO insert snapshot and conditions...

;; (define-test write-dsnapshot-checks-dstream-uuid
;;   (purge-destore)
;;   (create-dstreams)
;;   (purge-destore)
;;   (assert-error 'cl-postgres:database-error (with-connection (write-dsnapshot *dstream-a* 1 "{}"))))
  
;; (define-test read-last-dsnapshot-reads-last
;;   (purge-destore)
;;   (create-dstreams)
;;   (populate-destore)
;;   (with-connection
;;     (dotimes (i 100)
;;       (write-dsnapshot *dstream-a* (1+ i) "{}")))
;;   (assert-equal 100 (dsnapshot-version (with-connection (read-last-dsnapshot *dstream-a*)))))





;; (define-test write-devent-checks-dstream-type-key-rolls-back
;;   (purge-destore)
;;   (create-dstreams)
;;   (let ((dstream-type-key "FOO"))
;;     (with-connection (write-devent *dstream-a* dstream-type-key "com.example.devent-type" "{}" "{}"))
;;     (assert-error 'cl-postgres-error:unique-violation (with-connection (write-devent *dstream-b* dstream-type-key "com.example.devent-type" "{}" "{}")))
;;     (assert-equal 1 (with-connection (devent-version (write-devent *dstream-c* dstream-type-key "com.example.devent-type" "{}" "{}")))))
;;   (let ((last-devent (first (last (with-connection (read-devents *dstream-a* 0))))))
;;     (assert-equal 1 (devent-version last-devent)))
;;   (purge-destore)
;;   (create-dstreams)
;;   (let ((dstream-type-key "FOO"))
;;     (with-connection (write-devent *dstream-a* nil "com.example.devent-type" "{}" "{}"))
;;     (with-connection (write-devent *dstream-b* dstream-type-key "com.example.devent-type" "{}" "{}"))
;;     (assert-error 'cl-postgres-error:unique-violation (with-connection (write-devent *dstream-a* dstream-type-key "com.example.devent-type" "{}" "{}")))
;;     (assert-equal 1 (with-connection (devent-version (write-devent *dstream-c* dstream-type-key "com.example.devent-type" "{}" "{}")))))
;;   (let ((last-devent (first (last (with-connection (read-devents *dstream-a* 0))))))
;;     (assert-equal 1 (devent-version last-devent))))



;; (define-test write-devent-checks-devent-type-rolls-back
;;   (purge-destore)
;;   (create-dstreams)
;;   (with-connection (write-devent *dstream-a* nil "com.example.devent-type" "{}" "{}"))
;;   (assert-error 'cl-postgres-error:check-violation (with-connection (write-devent *dstream-a* nil "" "{}" "{}")))
;;   (let ((last-devent (first (last (with-connection (read-devents *dstream-a* 0))))))
;;     (assert-equal 1 (devent-version last-devent))))



;; ;; (define-test write-devent-checks-expected-version-rolls-back
;; ;;   (purge-destore)
;; ;;   (create-dstreams)
;; ;;   (assert-error 'cl-postgres:database-error (with-connection (write-devent *dstream-a* nil "com.example.devent-type" "{}" "{}")))
;; ;;   (purge-destore)
;; ;;   (create-dstreams)
;; ;;   (with-connection (write-devent *dstream-a* nil "com.example.devent-type" "{}" "{}"))
;; ;;   (with-connection (write-devent *dstream-a* nil "com.example.devent-type" "{}" "{}"))
;; ;;   (assert-error 'cl-postgres:database-error (with-connection (write-devent *dstream-a* nil "com.example.devent-type" "{}" "{}")))
;; ;;   (let ((last-devent (first (last (with-connection (read-devents *dstream-a* 0))))))
;; ;;     (assert-equal 2 (devent-version last-devent))))



;; (define-test write-dsnapshot-checks-dstream-uuid
;;   (purge-destore)
;;   (create-dstreams)
;;   (purge-destore)
;;   (assert-error 'cl-postgres:database-error (with-connection (write-dsnapshot *dstream-a* 1 "{}"))))



;; (define-test read-last-dsnapshot-reads-last
;;   (purge-destore)
;;   (create-dstreams)
;;   (populate-destore)
;;   (with-connection
;;     (dotimes (i 100)
;;       (write-dsnapshot *dstream-a* (1+ i) "{}")))
;;   (assert-equal 100 (dsnapshot-version (with-connection (read-last-dsnapshot *dstream-a*)))))
