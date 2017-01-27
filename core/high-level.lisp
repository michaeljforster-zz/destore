;;;; core/high-level.lisp

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

(defpackage "DESTORE/CORE/HIGH-LEVEL"
  (:use "CL")
  (:import-from "DESTORE/CORE/POSTGRES")
  (:export "RECONSTITUTE"
           "PROJECT"))

(in-package "DESTORE/CORE/HIGH-LEVEL")

(defun default-start-version (dsnapshot)
  (if (null dsnapshot)
      0
      (destore/core/postgres:dsnapshot-version dsnapshot)))

(defun default-initial-value (dsnapshot initial-value)
  (if (null dsnapshot)
      initial-value
      (destore/core/postgres:dsnapshot-payload dsnapshot)))

(defun reconstitute (function initial-value dstream)
  (let ((last-dsnapshot (destore/core/postgres:read-last-dsnapshot dstream)))
    (let ((start-version (default-start-version last-dsnapshot))
          (initial-value (default-initial-value last-dsnapshot initial-value)))
      (let ((history (destore/core/postgres:read-devents dstream start-version))
            (last-version start-version))
        (flet ((reducer (accumulator next)
                 (setf last-version (destore/core/postgres:devent-version next))
                 (funcall function accumulator next)))
          (let ((result (reduce #'reducer history :initial-value initial-value)))
            (values result last-version)))))))

(defun project (function initial-value dstream &key (start-version 0))
  (let ((history (destore/core/postgres:read-devents dstream start-version))
        (last-version start-version))
    (flet ((reducer (accumulator next)
             (setf last-version (destore/core/postgres:devent-version next))
             (funcall function accumulator next)))
      (let ((result (reduce #'reducer history :initial-value initial-value)))
        (values result last-version)))))
