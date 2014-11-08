(ns util.types
  (:require [clojure.string :as string]
            [clojure.set :as set]
            [util.time :as time])
  (:import [java.io.file]))

;; 'date-time
;; 'numeric
;; 'string
;; nil

(def ^:dynamic *category-threshold* 24)
(def ^:dynamic *default-date-format-str* "yyyy-mm-dd HH:mm:ss")

(defn string-alpha-numeric? [s]
  (re-matches #"[A-Za-z0-9\space\tab\-_\.\,]+" s))

(defn parse-number
  "Reads a number from a string. Returns nil if not a number."
  [s]
  (cond
   (string? s)
   (if (re-find #"^-?\d*\,?\d+\.?\d*([Ee]\+\d+|[Ee]-\d+|[Ee]\d+)?$" (.trim ^String s))
     (read-string (clojure.string/replace s "," "")))

   (number? s) s
   :default nil))

(defn string-numeric? [s]
  (parse-number s))

(defn string-sanitize [s]
  (if (string? s)
    (if (string/blank? s)
      nil
      (string/trim s))
    nil))

(defn detect-data-type
  "given a sample 'samp' returns the data type as a keyword. (date-time, numeric, nil, string)"
  [samp]
  (let [s (string-sanitize (str samp))]
    (cond
     (nil? s) nil
     (or
      (= "missing" (string/lower-case s))
      (= "nil" (string/lower-case s))
      (= "null" (string/lower-case s))
      (= "-" (string/lower-case s)))
     nil
     (or (string-numeric? s)) 'numeric
     ;; need to do a better job guessing date-time stamps
     (not (nil? (util.time/guess-date-format s ))) 'date-time
     (string? s) 'string
     :default nil)))

(defn guess-subtype [field-type uniques]
  (if (and
       (not (= field-type 'numeric))
       (<= uniques *category-threshold*))
    'category
    field-type))

(defn best-type-for-sequence
  "given a sequence s, returns the type for the values in the sequence"
  [s]
  (let [types-set (into #{} (map detect-data-type s))]
    (first (set/intersection types-set (sorted-set 'date-time 'numeric 'string 'nil)))))

;; todo: add the uniques check back in- even if it's from a small sample of the data
(defn best-subtype-for-sequence
  "given a sequence s, returns the type for the values in the sequence"
  [s -type]
  (guess-subtype -type 1))

(defmacro dynamic?
  [x]
  `(boolean (:dynamic (meta
                       (if (var? ~x)
                         '~x
                         #'~x) ))))
