 (ns util.time
    (:import [java.text SimpleDateFormat]
             [java.util Date]
             [java.util Calendar]
             [java.util.TimeZone])
    (:refer-clojure :exclude [second]))
;; deal with time as epoch seconds

(def ^:dynamic *default-format-str* "EEE, d MMM yyyy HH:mm:ss Z")

;; larger strings should be higher priority
(def ^:dynamic *date-format-list* ["EEE, d MMM yyyy HH:mm:ss Z"
                                   "EEE, d MMM yyyy HH:mm:ss"
                                   "yyyy-MM-dd'T'HH:mm:ss.SSS Z"
                                   "yyyy-MM-dd'T'HH:mm:ss.SSS"
                                   "yyyy-mm-dd HH:mm:ss Z"
                                   "yyyy-mm-dd HH:mm:ss"
                                   "dd/MM/yyyy:HH:mm:ss Z"
                                   "dd/MM/yyyy:HH:mm:ss"
                                   "dd/MMM/yyyy:HH:mm:ss Z"
                                   "dd/MMM/yyyy:HH:mm:ss"
                                   "MM/dd/yyyy:HH:mm:ss"
                                   "MM/dd/yy"
                                   "MM/dd/yyyy"])

(defn seconds-to-microseconds [s] (* s 1000000))
(defn seconds-to-milliseconds [s] (* s 1000))
(defn microseconds-to-seconds [ms] (/ ms 1000000))
(defn milliseconds-to-seconds [ms] (/ ms 1000))


(defn seconds "given seconds returns seconds"  [n] (* n 1))
(defn minutes "given minutes returns seconds" [n] (* (seconds n) 60))
(defn hours "given hours returns seconds" [n] (* (minutes n) 60))
(defn hours-minutes "given hours and minutes returns seconds" [h m] (+ (hours h) (minutes m)))
(defn days "returns seconds" [n] (* (hours n) 24))
(defn weeks "returns seconds" [n] (* (days n) 7))

(defn floor
  "returns timestamp rounded DOWN to the nearest interval.
Examples:
 round down to nearest 15 minute interval
 (floor t (minutes 15))

 round down to nearest day interval
 (floor t (days 1))"
  [timestamp interval]
  (- timestamp (mod timestamp interval)))

(defn ceiling
  "returns timestamp rounded UP to the nearest interval"
  [timestamp interval]
  (util.time/floor (+ timestamp interval) interval))

(defn to-milliseconds "given seconds return milliseconds" [n] (* n 1000))
(defn to-seconds "returns seconds from seconds" [n] (int n))
(defn to-minutes "returns minutes from seconds" [n] (int (/ n 60)))
(defn to-hours "returns hours from seconds" [n] (int (/ n 60 60)))
(defn to-days "returns hours from seconds" [n] (int (/ n 60 60 24)))
(defn to-years "returns years from seconds - this is naive.  doesn't use gregorian calendar"
  [n] (int (/ n 60 60 24 365)))

(defn utc-to-timezone [timestamp zonestr]
    "Converts a UTC timestamp to a timestamp in the specified time zone"
    (let [timezone (java.util.TimeZone/getTimeZone zonestr)
          msoffset (if (nil? timezone)
                      0
                      (. timezone getOffset (to-milliseconds timestamp)))
          offset (milliseconds-to-seconds msoffset)]
        (+ timestamp offset)))

(defn utc-from-string
  "Returns the number of (seconds/microseconds) since Midnight 1970 (UTC).
format-str is in SimpleDateFormat format."
  ([dtstring format-str milliseconds?]
     (let [ts
           (try
             (. (. (java.text.SimpleDateFormat. format-str) parse dtstring) getTime)
             (catch Exception e nil))]
       (if (nil? ts)
         nil
         (if milliseconds? ts (milliseconds-to-seconds ts)))))
  ([dtstring format-str] (utc-from-string dtstring format-str false))
  ([dtstring] (utc-from-string dtstring *default-format-str* false)))

(defn guess-date-format
  "return the best guess for a date format, or null if non is found"
  [dtstring]
  (->>
   (map (fn[test-str] (if (utc-from-string dtstring test-str) test-str nil)) *date-format-list*)
   (filter #(not (nil? %)))
   first))

(defn string-from-utc
  "returns a string in the format specified by format-str given the time UTC"
  ([utc format-str milliseconds?]
     (let [^Long ts (if milliseconds? utc (seconds-to-milliseconds utc))]
       (. (java.text.SimpleDateFormat. format-str) format (java.util.Date. ts))))
  ([utc format-str] (string-from-utc (int utc) format-str false))
  ([utc] (string-from-utc (int utc) *default-format-str* false)))

(defn string-to-time
  "returns an epoch time given the string. Attempts a best get at the format"
  [date-string]
  (if-let [fmt (guess-date-format date-string)]
    (utc-from-string date-string fmt)))

(defn date-time
  "returns a date time structure given an epoch time in seconds."
  [epoch]
  {:epoch epoch :date-str (string-from-utc epoch)})

(defn now
  "returns the time in seconds"
  [] (int  (milliseconds-to-seconds (.getTime
                                     (java.util.Date.)))))

(defn relative
  "converts absolute time t seconds into the number of seconds from now."
  [t]
  (- t (now)))

(defn from-now
  "returns an absolute epoch time"
  [seconds]
  (+ (now) seconds))

(defn calendar
  "returns a java calendar object initialized to right now"
  []
  (java.util.Calendar/getInstance))

(def ^:dynamic ^java.util.Calendar *calendar* nil)

(defmacro with-calendar [epoch & body]
  `(binding [*calendar* (java.util.Calendar/getInstance ~epoch)]
     ~@body))

(defn get-calendar-part
  ([^java.util.Calendar cal calendar-constant]
     (.get cal ^int calendar-constant))
  ([calendar-constant]
     (let [c (if *calendar*
               *calendar*
               (calendar))]
       (.get ^java.util.Calendar c  ^int calendar-constant))))

(defn hour [] (get-calendar-part java.util.Calendar/HOUR))
(defn minute [] (get-calendar-part java.util.Calendar/MINUTE))
(defn second [] (get-calendar-part java.util.Calendar/SECOND))
(defn day [] (get-calendar-part java.util.Calendar/DAY_OF_MONTH ))
(defn month [] (get-calendar-part java.util.Calendar/MONTH))
(defn year [] (get-calendar-part java.util.Calendar/YEAR))


(defn wait
  "waits delay number of seconds by placing the current thread to sleep."
  [delay]
  (Thread/sleep (to-milliseconds delay)))
