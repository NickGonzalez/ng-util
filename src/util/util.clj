(ns util.util
  (:require [clojure.pprint :as pprint])
  (:import java.util.UUID))

(def ^:dynamic *uuid-simple-not-really-unique-but-good-enough-for-development* false)

;; vector helpers
(defn padv
  "pads a vector v with n nils at the end"
  [v n & {:keys [pad-with] :or {pad-with nil}}] (into [] (concat v (repeat n pad-with))))

(defn ensure-sizev
  "ensures that vector v is of size n, if not add nils to the end of it until it is"
  [v n & {:keys [pad-with] :or {pad-with nil}}] (padv v (- n (count v)) :pad-with pad-with))

(defn fill-missing-vals
  "returns a list of rows guaranteed to have the same number of items"
  [rows]
  (let [n (count (first rows))]
    (mapv (fn [v] (ensure-sizev v n)) rows)))
;; ----------------------------------------------------------------------
;; math / vector helpers
;; ----------------------------------------------------------------------
(defn combine-vectors [v1 v2]
  (into [] (map (fn [val1 val2] (vector val1 val2)) v1 v2)))

(defn transpose
  "transposes a vector of vectors representing rows into a seq of vectors representing cols"
  [rows]
  (apply mapv vector rows))

(defn transpose-lazy
  "transposes a vector of vectors representing rows into a seq of vectors representing cols"
  [rows]
  (apply map vector rows))

(defn =-in
  "returns true if (get a kw) and (get b kw) are equal"
  [kw a b]
  (= (get a kw) (get b kw)))

(defn not-nil?
  "same as (not (nil? exp))"
  [exp] (not (nil? exp)))

(defn not-zero?
  "same as (not (nil? zero))"
  [exp] (not (zero? exp)))

;; useful when debugging
(defn ns-unmap-all
  "unmaps all public symbals in a namespace.  Defaults to the current namespace."
  ([ns]  (map ns-unmap (repeat ns) (map symbol (keys (ns-publics ns))) ))
  ([] (ns-unmap-all *ns*)))

(defmacro without-errors [fn]
  `(try ~fn
        (catch Exception e# (do (println "ignoring exception " e#) nil))
        (catch java.lang.AssertionError e# (do println "assertion error " e#) nil)))

;; (defn debug-break [x] (swank.core/break) x)
(defn debug-break [x] x)

(defn odd-rows [test-data]
  (filter #(not (nil? %)) (map (fn [idx row] (if (odd? idx) row)) (range 0 (count test-data)) test-data)))

(defn even-rows [test-data]
  (filter #(not (nil? %)) (map (fn [idx row] (if (even? idx) row)) (range 0 (count test-data)) test-data)))

;; helpful
(defn first-non-nil [seq]
  (first (filter #(not (nil? %)) seq)))

(defn sorted-map-by-val
  [coll]
  (into (sorted-map-by (fn [key1 key2]
                         (compare [(get coll key2) (hash key2)]
                                  [(get coll key1) (hash key1)])))
        coll))

(defn top-ten
  [coll fn-compare &
   {:keys [size]
    :or {size 10}}]
  (reduce (fn [top samp]
            (if (< (count top) size)
              (sort fn-compare (conj top samp))
              ;; if the current sample is larger then the smallest sample then add it
              (let [smallest (last top)]
                (if (fn-compare samp
                                smallest)
                  (sort fn-compare (conj (drop-last top)  samp))
                  top))))
          []
          coll))
;; math

(defn fround [x] (int (/ (+ 0.5 (- x (int x))) 1)))

(defn ffloor [x] (int x))

(defn fceiling [x] (+ 1 (int x)))

(defn has-keys [m keys]
  (reduce (fn [ret key]
            (and ret (get m key)))
          keys))
(defn maybe
  "if x is not nil returns x, otherwise returns y "
  [x y]
  (if-not (nil? x) x y))

(defn to-string [id]
  (cond
   (string? id) id
   (keyword? id) (str (name id))
   (symbol? id) (str (name id))
   :default (str id)))

(defn to-kw [id]
  (cond
   (string? id) (keyword id)
   (keyword? id) id
   (symbol? id) (keyword (name id))
   :default (keyword id) ))

(defn to-symbol [id]
  (cond
   (string? id) (symbol id)
   (keyword? id) (symbol (name id))
   (symbol? id) id
   :default (symbol id) ))

(defn throw-if
  "same as assert"
  [test description]
  (when test (throw (Throwable. ^String description)))
  test)

(defn throw-if-nil [test description]
  (when (nil? test ) (throw (Throwable. ^String description)) )
  test)

(def tin throw-if-nil)

(defn call-fn
  "calls a fn function - specified by string - with the values in the params vector"
  [ns function & params]
  (def d (count params))
  (when-let [f (ns-resolve (symbol ns)
                                    (symbol  (str ns "/" function) ))]
    (let [c (count params)
          m (meta f)]
      (def c2 c)
      (cond
       (= c 0) (with-meta (f) m)
       (= c 1) (with-meta (f (first params)) m)
       (= c 2) (with-meta (f (first params) (second params)) m)
       :else (with-meta (apply f (first params) (rest params)) m)))))

(defn call-fnv [ns function args-v]
  (def c (count args-v))
  (apply call-fn ns function args-v))

(defn pid
  "returns the pid of the current process"
  []
  (let [proc-name (.getName (java.lang.management.ManagementFactory/getRuntimeMXBean))
        end-idx (.indexOf proc-name "@")]
    (if (> end-idx -1)
      (->
       (.substring ^String proc-name 0 end-idx)
       (Integer/parseInt)))))


(defn map-values [-fn -coll]
  (mapv -fn (keys -coll)))

(defn map-keys [-fn -coll]
  (mapv -fn (vals -coll)))

(defn options-string-from-map
  "returns a string in the format 'k1:v1, k2:v2, .."
  [opts]
  (apply str (interpose ", " (map (fn [[k v]] (str (to-string k) ":"
                                                   (if (string? v)
                                                     (str "'" v "'")
                                                     v)))
                                  (into [] opts))))  )

(defn ppassert-printv [ argsv]
  (with-out-str
    (doseq [x argsv]
      (pprint/pprint x))))

(defmacro ppassert [ test args ]
  `(assert ~test (ppassert-printv ~args)))


(defmacro trace-args
  "write a pprint for each arg in the arg-v vector"
  ([ arg-v ]
  `(ppassert-printv ~arg-v))
  ([s arg-v]
     `(println s)
     `(ppassert-printv ~arg-v)))


(defn str-to-bytes [s]
  (byte-array (mapv byte (into [] s))) )

(defn bytes-to-str [b]
  (apply str (map char b)))

(defn get-time-ms [] (.getTime (java.util.Date.)))

(defmacro ->progress [param sexp tag]
  `(do
     (println "====================================")
     (println (str "Begin -- [" ~tag "] --"))
     ;; do it
     (let [ res# (-> ~param ~sexp) ]
       (println (str "End -- [" ~tag "] --"))
       (println "====================================")
       res#)))

(defmacro progress [exp tag]
  `(do
     (println "====================================")
     (println (str "Begin -- [" ~tag "] --"))
     ;; do it
     (let [ res# ~exp ]
       (println (str "End -- [" ~tag "] --"))
       (println "====================================")
       res#)))

(defn println->
  "used when using a threading macro -> and wanting to print out the
  data within the chain of calls"
  [item s]
         (println s item)
         item)

(defn println->>
  "used when using a threading macro ->> and wanting to print out the
  data within the chain of calls"
  [s item]
         (println s item)
         item)

(defn between?
  [i low high]
  (and (>= i low) (< i high)))

(defn to-hex
  "single digit hex conversion "
  [i]
  (assert (between? i 0 16))
  (let [lu {10 "A" 11 "B" 12 "C" 13 "D" 14 "E" 15 "F"}]
    (cond
     (< i 10) (str i)
     (between? i 10 16) (get lu i)
     :else (throw (Exception. "int out of bounds for hex conversion")))))

(defn rand-hex-digit
  "returns a random digit from 0-F"
  []
  (to-hex (rand-int 16)))

(defn rand-str [len]
  (apply str (map (fn[x] (rand-hex-digit)) (range 0 len))))

(defn rand-id [prefix & {:keys [digits] :or {digits 8} }]
  (apply str prefix (repeatedly digits #(int (rand 10)))))

(defn fake-uuid []
  (rand-str 16))

(defn real-uuid []
  (str (java.util.UUID/randomUUID)))

(if *uuid-simple-not-really-unique-but-good-enough-for-development*
  (def uuid fake-uuid)
  (def uuid real-uuid))


(defn remove-last
   [s] (.substring ^String s 0 (dec (count s))))

(defmacro with-params
  "deconstructs param-map and makes names a valid list of aliases to use"
  [[ names-vec param-map] & rest]
  `(let [{:keys `~names} ~param-map]
     ~@rest))

(defn mk-thread
  "returns a thread object ready to call thread-fn"
  [thread-fn]
  (Thread. ^java.lang.Runnable thread-fn))

(defn run-thread
  "starts a thread object returned by thread"
  [thread]
  (doto ^java.lang.Thread thread
    (.run)))

(defn spawn-thread
  "makes and starts a new thread"
  [th-fn]
  (doto
   (mk-thread th-fn)
   run-thread))


(defn identity-if
  "returns obj if test is true.  test is a function which takes one parameter - obj. otherwise returns false"
  [obj test-fn]
  (and (test-fn obj) obj))


(defn ident-test
  "returns a function that takes a single parameter obj. if (test-fn obj) is true, returns obj otherwise returns false."
  [test-fn]
  #(and (test-fn %) %))


(defmacro into-kv
  "returns a map with a single item named the variable name of x and set to the actual value of x"
  [x]
  `(hash-map (keyword (name '~x)) ~x))

(defn object-id-to-int
  ([object-id max]
   (if (zero? max)
     0
     (mod (object-id-to-int object-id) max)))
  ([object-id]
    (reduce (fn [agg row] (+ agg  (int row) )) 0 object-id)))

(defn compute-md5-long
  "returns a long value containing the top 64 bits of the MD5 hash of the input string"
  [^String str-value]
  (let [digest (java.security.MessageDigest/getInstance "MD5")
        bytes (vec (.digest digest (.getBytes str-value)))]
    (reduce
      #(bit-or %1
        (bit-shift-left
          (long (bit-and (get bytes %2) 0xff))
          (* (- 7 %2) 8)))
      0
      (range 8))))

(defn create-random-seq
  "create a lazy sequence of random values using a specified seed (string) and a function to select a value from a java.util.Random instance"
  [seed-string val-fn]
  (let [seed (compute-md5-long seed-string)
        random (java.util.Random. seed)]
    (repeatedly #(val-fn random))))

(defn create-random-double-seq
  "create a lazy sequence of random double values using a specified seed (string)"
  [seed-string]
  (create-random-seq seed-string (fn [^java.util.Random rnd] (.nextDouble rnd))))

(defn create-random-int-seq
      "create a lazy sequence of random int value (0 <= r < max) using a specified seed (string)"
      [seed-string max]
      (create-random-seq seed-string (fn [^java.util.Random rnd] (.nextInt rnd max))))

(defn min-by [f coll]
  "returns the entry in coll that minimizes (f entry)"
  (if (empty? coll)
    nil
    (reduce (fn [acc x]
              (if (< (f x) (f acc))
                x
                acc))
            (first coll)
            coll)))

(defn max-by
  "returns the entry in coll that maximizes (f entry). "
  [f coll]
  (if (empty? coll)
    nil
    (reduce (fn [acc x]
              (if (> (f x) (f acc))
                x
                acc))
            (first coll)
            coll)))

(defn sanitize
  "converts spaces to -"
  [s]
  (to-kw (clojure.string/replace s #" " "-" )))

(defn seq->map
  "converts a sequence of objs to a map of objs with the map key being the return value from f"
  [f coll]
  (apply conj (map #(hash-map (f %) %) coll)))

(defn with-retries
  "executes the code retry-count number of times (or unlimited if retry-count is nil) until it returns true.
   Will call fail with the retry count on failure and success with the retry count on success"
  [f & {:keys [fail success retry-count]} ]
  (loop [retries 1]
    (let [success? (f)]
      (cond
        success? (if (ifn? success) (success retries))
        (and (not (nil? retry-count)) (>= retries retry-count)) (if (ifn? fail) (fail retries))
        :else (recur (inc retries))))))
