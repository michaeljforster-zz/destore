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

(define-test insert-drefs
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

;; TODO (DESTORE/CORE/POSTGRES:INSERT-DEVENT-RETURNING )

;; TODO snapshot




;; (define-test list-all-find-dstreams
;;   (purge-destore)
;;   (create-dstreams)
;;   (populate-destore)
;;   (with-connection
;;     (let ((dstreams (list-all-dstreams)))
;;       (let ((first-dstream (first dstreams)))
;;         (let ((first-dstream-uuid (dstream-uuid first-dstream)))
;;           (let ((found-dstream (find-dstream first-dstream-uuid)))
;;             (assert-true (uuid:uuid= first-dstream-uuid
;;                                      (dstream-uuid found-dstream)))))))))

;; (define-test list-all-devents-ascending-sequence-no
;;   (purge-destore)
;;   (create-dstreams)
;;   (populate-destore)
;;   (let ((devents (with-connection (list-all-devents)))
;;         (sequence-no 0))
;;     (dolist (devent devents)
;;       (assert-true (< sequence-no (devent-sequence-no devent)))
;;       (setf sequence-no (devent-sequence-no devent)))))

;; (define-test read-devents-correct-count
;;   (purge-destore)
;;   (create-dstreams)
;;   (populate-destore)
;;   (assert-equal *dstream-size-a* (length (with-connection (read-devents *dstream-a* 0))))
;;   (assert-equal *dstream-size-b* (length (with-connection (read-devents *dstream-b* 0)))))

;; (define-test read-devents-correct-dstream
;;   (purge-destore)
;;   (create-dstreams)
;;   (populate-destore)
;;   (assert-true (every #'(lambda (devent) (uuid:uuid= (dstream-uuid *dstream-a*)
;;                                                      (devent-dstream-uuid devent)))
;;                       (with-connection (read-devents *dstream-a* 0)))))

;; (define-test read-devents-versions-monotonically-nondecreasing
;;   (purge-destore)
;;   (create-dstreams)
;;   (populate-destore)
;;   (let ((initial-version 50))
;;     (let ((devents (with-connection (read-devents *dstream-a* initial-version)))
;;           (version initial-version))
;;       (assert-equal initial-version (devent-version (first devents)))
;;       (dolist (devent devents)
;;         (assert-true (<= version (devent-version devent)))
;;         (setf version (devent-version devent))))))

;; (define-test write-devent-increments-dstream-version
;;   (purge-destore)
;;   (create-dstreams)
;;   (assert-equal 0 (dstream-version *dstream-a*))
;;   (with-connection (write-devent *dstream-a* nil "com.example.devent-type" "{}" "{}"))
;;   (assert-equal 1 (dstream-version *dstream-a*))
;;   (with-connection (write-devent *dstream-a* nil "com.example.devent-type" "{}" "{}"))
;;   (assert-equal 2 (dstream-version *dstream-a*)))

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
