;;;; destore.asd

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

#-asdf3.1 (error "DESTORE requires ASDF 3.1 or later. Please upgrade your ASDF.")

(asdf:defsystem "destore"
  :description "Domain Event Store written in Common Lisp and PostgreSQL"
  :author "Michael J. Forster <mike@forsterfamily.ca>"
  :license "MIT"
  :version "0.0.1"
  :class :package-inferred-system
  :defsystem-depends-on ("asdf-package-system")
  :depends-on ("destore/core/all")
  :in-order-to ((test-op (test-op "destore/test"))))

(asdf:defsystem "destore/test"
  :depends-on ("destore/test/all")
  :perform (test-op (o c)
                    (uiop:symbol-call "LISP-UNIT"
                                      "RUN-TESTS"
                                      :all
                                      "DESTORE/TEST/ALL")))
