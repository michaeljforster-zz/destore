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
  (:use "CL"
        "LISP-UNIT"
        "DESTORE/CORE/POSTGRES"
        "DESTORE/TEST/SUPPORT"))

(in-package "DESTORE/TEST/POSTGRES")

(defparameter *dstream-uuid-a*
  (fresh-uuid))

(defparameter *dstream-uuid-b*
  (fresh-uuid))

(defparameter *dstream-size-a* 100)

(defparameter *dstream-size-b* 100)

(defun populate-destore ()
  (with-connection
    (dotimes (i *dstream-size-a*)
      (write-devent *dstream-uuid-a* "com.example.dstream-type-a" nil i (fresh-uuid) "com.example.devent-type" "{}" "{}"))
    (dotimes (i *dstream-size-b*)
      (write-devent *dstream-uuid-b* "com.example.dstream-type-b" nil i (fresh-uuid) "com.example.devent-type" "{}" "{}"))))

(define-test empty-destore
  (purge-destore)
  (with-connection ()
    (assert-equal '() (list-all-dstreams))
    (assert-equal '() (list-all-devents))
    (assert-equal '() (read-devents *dstream-uuid-a* 0))
    (assert-equal nil (read-last-dsnapshot *dstream-uuid-a*))))

(define-test list-all-devents-ascending-sequence-no
  (purge-destore)
  (populate-destore)
  (let ((devents (with-connection (list-all-devents)))
        (sequence-no 0))
    (dolist (devent devents)
      (assert-true (< sequence-no (devent-sequence-no devent)))
      (setf sequence-no (devent-sequence-no devent)))))

(define-test read-devents-correct-count
  (purge-destore)
  (populate-destore)
  (assert-equal *dstream-size-a* (length (with-connection (read-devents *dstream-uuid-a* 0))))
  (assert-equal *dstream-size-b* (length (with-connection (read-devents *dstream-uuid-b* 0)))))

(define-test read-devents-correct-dstream
  (purge-destore)
  (populate-destore)
  (assert-true (every #'(lambda (devent) (uuid:uuid= *dstream-uuid-a* (devent-dstream-uuid devent)))
                      (with-connection (read-devents *dstream-uuid-a* 0)))))

(define-test read-devents-versions-monotonically-nondecreasing
  (purge-destore)
  (populate-destore)
  (let ((initial-version 50))
    (let ((devents (with-connection (read-devents *dstream-uuid-a* initial-version)))
          (version initial-version))
      (assert-equal initial-version (devent-version (first devents)))
      (dolist (devent devents)
        (assert-true (<= version (devent-version devent)))
        (setf version (devent-version devent))))))

(define-test write-devent-checks-dstream-type-rolls-back
  (purge-destore)
  (assert-error 'cl-postgres-error:check-violation (with-connection (write-devent *dstream-uuid-a* "" nil 0 (fresh-uuid) "com.example.devent-type" "{}" "{}")))
  (assert-equal '() (with-connection (read-devents *dstream-uuid-a* 0)))
  (purge-destore)
  (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" nil 0 (fresh-uuid) "com.example.devent-type" "{}" "{}"))
  (assert-error 'cl-postgres:database-error (with-connection (write-devent *dstream-uuid-a* "" nil 1 (fresh-uuid) "com.example.devent-type" "{}" "{}")))
  (let ((last-devent (first (last (with-connection (read-devents *dstream-uuid-a* 0))))))
    (assert-equal 1 (devent-version last-devent))))

(define-test write-devent-checks-dstream-type-key-rolls-back
  (purge-destore)
  (let ((dstream-type-key "FOO"))
    (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" dstream-type-key 0 (fresh-uuid) "com.example.devent-type" "{}" "{}"))
    (assert-error 'cl-postgres-error:unique-violation (with-connection (write-devent *dstream-uuid-b* "com.example.dstream-type-a" dstream-type-key 0 (fresh-uuid) "com.example.devent-type" "{}" "{}"))))  
  (let ((last-devent (first (last (with-connection (read-devents *dstream-uuid-a* 0))))))
    (assert-equal 1 (devent-version last-devent)))
  (purge-destore)
  (let ((dstream-type-key "FOO"))
    (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" nil 0 (fresh-uuid) "com.example.devent-type" "{}" "{}"))
    (with-connection (write-devent *dstream-uuid-b* "com.example.dstream-type-a" dstream-type-key 0 (fresh-uuid) "com.example.devent-type" "{}" "{}"))
    (assert-error 'cl-postgres-error:unique-violation (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" dstream-type-key 1 (fresh-uuid) "com.example.devent-type" "{}" "{}"))))
  (let ((last-devent (first (last (with-connection (read-devents *dstream-uuid-a* 0))))))
    (assert-equal 1 (devent-version last-devent))))

(define-test write-devent-checks-devent-type-rolls-back
  (purge-destore)
  (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" nil 0 (fresh-uuid) "com.example.devent-type" "{}" "{}"))
  (assert-error 'cl-postgres-error:check-violation (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" nil 1 (fresh-uuid) "" "{}" "{}")))
  (let ((last-devent (first (last (with-connection (read-devents *dstream-uuid-a* 0))))))
    (assert-equal 1 (devent-version last-devent))))

(define-test write-devent-checks-expected-version-rolls-back
  (purge-destore)
  (assert-error 'cl-postgres:database-error (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" nil 1 (fresh-uuid) "com.example.devent-type" "{}" "{}")))
  (purge-destore)
  (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" nil 0 (fresh-uuid) "com.example.devent-type" "{}" "{}"))
  (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" nil 1 (fresh-uuid) "com.example.devent-type" "{}" "{}"))
  (assert-error 'cl-postgres:database-error (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" nil 1 (fresh-uuid) "com.example.devent-type" "{}" "{}")))
  (let ((last-devent (first (last (with-connection (read-devents *dstream-uuid-a* 0))))))
    (assert-equal 2 (devent-version last-devent))))

(define-test write-devent-checks-devent-uuid-rolls-back
  (purge-destore)
  (let ((devent-uuid (fresh-uuid)))
    (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" nil 0 devent-uuid "com.example.devent-type" "{}" "{}"))
    (assert-error 'cl-postgres-error:unique-violation (with-connection (write-devent *dstream-uuid-a* "com.example.dstream-type-a" nil 1 devent-uuid "com.example.devent-type" "{}" "{}"))))
  (let ((last-devent (first (last (with-connection (read-devents *dstream-uuid-a* 0))))))
    (assert-equal 1 (devent-version last-devent))))

(define-test write-dsnapshot-checks-dstream-uuid
  (purge-destore)
  (assert-error 'cl-postgres-error:foreign-key-violation (with-connection (write-dsnapshot (fresh-uuid) 1 "{}")))
  (assert-error 'cl-postgres-error:foreign-key-violation (with-connection (write-dsnapshot *dstream-uuid-a* 1 "{}"))))
  
(define-test read-last-dsnapshot-reads-last
  (purge-destore)
  (populate-destore)
  (with-connection
    (dotimes (i 100)
      (write-dsnapshot *dstream-uuid-a* (1+ i) "{}")))
  (assert-equal 100 (dsnapshot-version (with-connection (read-last-dsnapshot *dstream-uuid-a*)))))
