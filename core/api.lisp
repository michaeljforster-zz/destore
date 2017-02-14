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

;;; This module provides the API for the destore.

(defpackage "DESTORE/CORE/API"
  (:use "CL"
         "DESTORE/CORE/POSTGRES")
  (:export "CREATE-DREF"
           "COUNT-DREFS"
           "LIST-DREFS"
           "FIND-DREF"
           "RECORD-DEVENT"
           "COUNT-DEVENTS"
           "SNAPSHOT"
           "COUNT-DSNAPSHOTS"
           "RECONSTITUTE"
           "PROJECT"))

(in-package "DESTORE/CORE/API")

(defstruct dref uuid type)

(defun create-dref (dref-type)
  (let ((dref-uuid (genuuid)))
    (insert-dref dref-uuid dref-type)
    dref-uuid))

(defun row-to-dref (row)
  (make-dref :uuid (first row) :type (second row)))

(defun count-drefs (&optional dref-type)
  (if (null dref-type)
      (%count-drefs)
      (%count-drefs-of-type dref-type)))

(defun list-drefs (&key dref-type)
  (if (null dref-type)
      (mapcar #'row-to-dref (select-drefs))
      (mapcar #'row-to-dref (select-drefs-of-type dref-type))))

(defun find-dref (dref-uuid)
  (row-to-dref (select-dref dref-uuid)))

(defun record-devent (dref expected-version devent-type payload &key secondary-key-value metadata)
  (let ((devent-uuid (genuuid)))
    (let ((version (insert-devent-returning (dref-uuid dref)
                                            expected-version
                                            secondary-key-value
                                            devent-uuid
                                            devent-type
                                            metadata
                                            payload)))
      (values devent-uuid version))))

(defun count-devents (&optional dref-uuid (version 1))
  (if (null dref-uuid)
      (count-devents)
      (count-devents-for-dref-starting dref-uuid version)))

(defun reduce-devents (function initial-value dref-uuid start-version)
  (let ((history (select-devents-for-dref-starting dref-uuid start-version))
        (last-version (max 0 start-version)))
    (flet ((reducer (accumulator next)
             (let ((devent-uuid (nth 0 next))
                   (devent-type (nth 1 next))
                   (metadata (nth 2 next))
                   (payload (nth 3 next))
                   ;; (dref-uuid (nth 4 next)) ; hidden detail
                   (version (nth 5 next))
                   ;; (stored-when (nth 6 next)) ; hidden detail
                   ;; (sequence-no (nth 7 next)) ; hidden detail
                   )
               (setf last-version version)
               (funcall function
                        accumulator
                        devent-uuid
                        devent-type
                        metadata
                        payload
                        ;; dref-uuid ; hidden detail
                        version
                        ;; stored-when ; hidden detail
                        ;; sequence-no ; hidden detail
                        ))))
      (let ((result (reduce #'reducer history :initial-value initial-value)))
        (values result last-version)))))

(defun dsnapshot-version (row default)
  (if (null row)
      default
      (second row)))

(defun dsnapshot-payload (row default)
  (if (null row)
      default
      (third row)))

(defun snapshot (dref function initial-value)
  (let ((last-dsnapshot (select-dsnapshots (dref-uuid dref) t)))
    (let ((initial-value (dsnapshot-payload last-dsnapshot initial-value))
          (start-version (1+ (dsnapshot-version last-dsnapshot 0))))
      (multiple-value-bind (payload version)
          (reduce-devents function initial-value dref start-version)
        (insert-dsnapshot (dref-uuid dref) version payload)))))

(defun count-snapshots (&optional dref-uuid)
  (if (null dref-uuid)
      (count-dsnapshots)
      (count-dsnapshots-for-dref dref-uuid)))

(defun reconstitute (dref function initial-value)
  (let ((last-dsnapshot (select-dsnapshots (dref-uuid dref) t)))
    (let ((initial-value (dsnapshot-payload last-dsnapshot initial-value))
          (start-version (1+ (dsnapshot-version last-dsnapshot 0))))
      (reduce-devents function initial-value dref start-version))))

(defun project (dref function initial-value &key (start-version 1))
  (reduce-devents function initial-value dref start-version))
