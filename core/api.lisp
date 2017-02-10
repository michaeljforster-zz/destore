;;;; core/api.lisp

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

(defpackage "DESTORE/CORE/API"
  (:use "CL"
         "DESTORE/CORE/POSTGRES")
  (:export "CREATE-DREF"
           "LIST-DREFS"
           "RECORD"
           "RECONSTITUTE"
           "SNAPSHOT"
           "PROJECT"))

(in-package "DESTORE/CORE/API")

;; NOTE: in this layer, we handle a dref as a UUID and type--symmetry
;; with creation; provides neither version nor secondary-key, which
;; will be provided--when essential--during reconstitution or
;; projection.

(defun reduce-devents (function initial-value dref-uuid start-version)
  (let ((history (select-devents dref-uuid start-version))
        (last-version (max 0 start-version)))
    (flet ((reducer (accumulator next)
             (let ((devent-uuid (nth 0 next))
                   (devent-type (nth 1 next))
                   (metadata (nth 2 next))
                   (payload (nth 3 next))
                   ;; (dref-uuid (nth 4 next))
                   (version (nth 5 next))
                   ;; (stored-when (nth 6 next))
                   ;; (sequence-no (nth 7 next))
                   )
               (setf last-version version)
               (funcall function
                        accumulator
                        devent-uuid
                        devent-type
                        metadata
                        payload
                        ;; dref-uuid
                        version
                        ;; stored-when
                        ;; sequence-no
                        ))))
      (let ((result (reduce #'reducer history :initial-value initial-value)))
        (values result last-version)))))

(defstruct dref uuid type)

(defun create-dref (dref-type)
  (let ((dref-uuid (genuuid)))
    (insert-dref dref-uuid dref-type)
    dref-uuid))

(defun row-to-dref (row)
  (make-dref :uuid (first row) :type (second row)))

(defun list-drefs (&key dref-type)
  (mapcar #'row-to-dref (select-drefs dref-type)))

(defun record (dref expected-version devent-type payload &key secondary-key-value metadata)
  (let ((devent-uuid (genuuid)))
    (let ((version (insert-devent-returning (dref-uuid dref)
                                            expected-version
                                            secondary-key-value
                                            devent-uuid
                                            devent-type
                                            metadata
                                            payload)))
      (values devent-uuid version))))

(defun dsnapshot-version (row default)
  (if (null row)
      default
      (second row)))

(defun dsnapshot-payload (row default)
  (if (null row)
      default
      (third row)))

(defun reconstitute (dref function initial-value)
  (let ((last-dsnapshot (select-last-dsnapshot (dref-uuid dref))))
    (let ((initial-value (dsnapshot-payload last-dsnapshot initial-value))
          (start-version (1+ (dsnapshot-version last-dsnapshot 0))))
      (reduce-devents function initial-value dref start-version))))

(defun snapshot (dref function initial-value)
  (let ((last-dsnapshot (select-last-dsnapshot (dref-uuid dref))))
    (let ((initial-value (dsnapshot-payload last-dsnapshot initial-value))
          (start-version (1+ (dsnapshot-version last-dsnapshot 0))))
      (multiple-value-bind (payload version)
          (reduce-devents function initial-value dref start-version)
        (insert-dsnapshot (dref-uuid dref) version payload)))))

(defun project (dref function initial-value &key (start-version 1))
  (reduce-devents function initial-value dref start-version))
