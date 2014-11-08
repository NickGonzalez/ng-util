(ns util.io
  (:import [java.text SimpleDateFormat]
           [java.util Date]
           [java.io File FileReader StringReader OutputStreamWriter InputStreamReader])
  (:require [util.types :as types]
            [util.util :as util]
            [clojure.string :as string]
            [clojure.java.io :as cio]
            [clojure.java.shell :refer [sh]]
            [clojure.pprint :as pprint]))


;; ----------------------------------------------------------------------
;; global config - defaults
;; ----------------------------------------------------------------------
(constantly (def cache-file-path  "/tmp/bigdata-cache/" ))
(def ^:dynamic *limit-file-size* false)

;; ----------------------------------------------------------------------
;; printing helpers
;; ----------------------------------------------------------------------


(defn url-to-path
  "returns a path given a url object, only if it is a file:/"
  [^java.net.URL _url]
  (if (= (.getProtocol ^java.net.URL _url) "file")
    (.getFile _url)
    nil))

(defn to-path
  [_path]
  (cond
   (string? _path) _path
   (= (type _path) java.net.URL) (url-to-path _path)
   :else
   (throw (Exception. "invalid type passed into to-path"))))

(defn mk-writer
  "returns a java.io.Writer from file name"
  [^String _path]
  (java.io.FileWriter. ^String (to-path _path)))
;; ----------------------------------------------------------------------
;; serialization
;; ----------------------------------------------------------------------
(defn serialize
  "Serialize an s-exp to a file"
  [sexp file & {:keys [write-fn] :or {write-fn prn} }]
  (with-open [^java.io.FileWriter w (mk-writer file)]
    (binding [*out* w *print-dup* true]
      (write-fn sexp)))
  sexp)

(defn serialize-seq
  "serializes a sequence to a file one line at a time"
  [sexp file & {:keys [write-fn]
                :or {write-fn prn}}]
  (with-open [w (clojure.java.io/writer file) ]
    (doseq [l sexp]
      (binding [*out* w *print-dup* true]
        (let [rows (get l :rows) ]
          (cond
           (and (not (nil? rows)) (vector? rows))  (doall (map #(if (not (nil? %)) (write-fn %)) rows))
           (not (nil? l)) (write-fn l)))))))



(defn serialize-string [sexp] (with-out-str (pr sexp)))
(defn deserialize-string [_s] (read-string _s))

(defn deserialize-1
  "returns nil if _file is not found, otherwise deserielizes the contents of _file and returns the first form found.
Slurp version"
  [ file ]
  (try
    (when-let [s (slurp (to-path file))]
      (if-not (string/blank? s)
        (with-in-str s (read))
        nil))
    (catch Exception e nil)))

(defn deserialize-2
  "deserialize - pushback reader version"
  [ file ]
  (binding [*read-eval* false]
    (with-open [r (java.io.PushbackReader. (FileReader. ^String (to-path file) ))]
      (read r))))

(def deserialize deserialize-1)


;; ----------------------------------------------------------------------
;; csv file helpers
;; ----------------------------------------------------------------------
(defn parse-csv-line
  "returns a vector of strings after parsing line with delimitted by separator.
If width is passed in - ensure the vector has width number of fields.  Pads the vector with nil fields if it is too small"
  [line &
   {:keys [separator width]
    :or {separator "," width nil}}]
  (if-not (nil? width)
    (util/ensure-sizev (string/split line (re-pattern separator)) width)
    (string/split line (re-pattern separator))))

(defn parse-log-line
  "parse a log line separated by :separators enclosed by :enclosing-map"
  [log-str &
   {:keys [enclosing separators]
    :or {enclosing {\[ \] \" \"}
         separators #{ \space \newline \tab}}}]
  (loop [line (apply vector log-str)
         field []
         result []
         closing-char nil]
    (let [c (first line)]
      (cond
       ;; nili is the end of the string
       (nil? c) (if (> (count field) 0)
                  (conj result (apply str field) )
                  result)
       ;; we are in an enclosed string -- add everything to the field until we find the close
       (not (nil? closing-char))
       (cond
        ;; we are no longer in an enclosure - just end it.
        ;; the field will end with the next separator
        (= c closing-char)
        (recur (rest line)
               field
               result
               nil)
        :default
        (recur (rest line)
               (conj field c)
               result
               closing-char))
       ;; check to see if we need to start an enclosed string
       (contains? enclosing c)
       (recur (rest line)
              field
              result
              (get enclosing c))
       ;; now check for the field separators so we can commit the field to our list
       (contains? separators c)
       (recur  (rest line)
               []
               (conj result (apply str field) )
               closing-char)
         ;;; regular characters just get added to the current field
       :default (recur
                 (rest line)
                 (conj field c)
                 result
                 closing-char)))
    ))

(defn to-delimitted-string [coll delim] (reduce (fn [s i] (str s delim i)) (first coll) (rest coll)))


(defn to-delimitted [data delim]
  (reduce (fn [s x] (if (nil? s) x (str s delim  x))) nil data))

(defn to-csv [data]
  (to-delimitted (map #(to-delimitted % ", ") data) "\n"))


;; (defn to-csv [data]
;;   (to-delimitted-string
;;    (map #(to-delimitted-string % ", ") data)
;;    "\n"))

(defn read-csv-with-reader
  [r &
   {:keys [separators enclosing lines line-end]
    :or {lines 100 }}]
  {:pre [(if (nil? lines) true (> lines 0))
         (complement (nil? r)) ]}
  (let [seq (line-seq r)
        ;; force max lines to be 1000
        lines (if *limit-file-size*
                (if-not (and lines (< lines 10000)) 10000 lines)
                lines)]
    (loop [file-seq seq
           v []
           n 0]
      (if (and file-seq
               (not (empty? file-seq))
               (or (and lines (< n lines))
                   (not lines))) ;; don't pay attention to the count if lines is nil

        (recur (rest file-seq)
               (conj
                v
                (parse-log-line (first file-seq)
                                :enclosing enclosing
                                :separators separators))
               ;; (string/split (first file-seq) (re-pattern separator)))
               (inc n))
        v))))


(defn path? [s] (and (string? s)
                     (not (clojure.string/blank? s))
                     (not (nil? (re-find #"[/|\\]" s)))))


(defn read-csv
  "returns a rowm matrix for a data / csv file pointed to by path.
Optional parameters
===================
separators is a set of characters that separate fields. defaults to , .
enclosing is a map of begin character to end character for enclosing fields that contain 'separators' in them."
  ([path &
    {:keys [separators enclosing line-end max-lines header]
     :or {separators #{ ","}
          line-end #{"\n"} }}]
     (with-open [r (cond
                    (string? path) (java.io.BufferedReader. (FileReader. ^String path))
                    (= (type path) java.net.URL) (java.io.BufferedReader.
                                                  (InputStreamReader. (.openStream ^java.net.URL path))))]
       (read-csv-with-reader r
                             :separators separators
                             :enclosing enclosing
                             :lines max-lines
                             :line-end line-end)))
  ([data-src] (read-csv (:in-path data-src)
                        :separators (:separators data-src)
                        :enclosing (:enclosing data-src)
                        :lines (:max-lines data-src)
                        :line-end (:line-end data-src))))

(defn read-csv-string
  "returns a row matrix for a csv 'file' loaded into a string"
  [s & {:keys [separators numlines] :or {separators #{\,} numlines 5}}]
  (with-open [r (java.io.BufferedReader. (StringReader. s))]
    (read-csv-with-reader r :separators separators :lines numlines)))

;; this can probably be improved by considering separators first and then enclosing
(defn detect-file-structure
  "Returns a map with :enclosing = map of opening enclosing chars to closing enclosing chars
 and :separators = a set of separator characters"
  [path & {:keys [max-lines]
           :or {max-lines 10}}]
  (let [separators-candidates [ #{ \space } #{ \, } #{ \tab } #{ \space \,} ]
        enclosing-candidates [ { \" \" } { \[ \] } { \' \'} { \{ \} }
                               { \" \" \[ \]}]

        ;; make a lazy list of all possibilities
        candidates (flatten
                    (for [ separators-candidate separators-candidates ]
                      (for [ enclosing-candidate enclosing-candidates ]
                        {:separators separators-candidate
                         :enclosing enclosing-candidate})))]
    ;; narrow down candidates to those that are consistent
    (->> (map (fn [ {separators :separators enclosing :enclosing} ]
                {:structure {:separators separators
                             :enclosing enclosing}
                 :data (read-csv path
                                 :separators separators
                                 :enclosing enclosing
                                 :max-lines 10)})
              candidates)
         (map (fn [candidate]
                (assoc candidate
                  :counts (mapv (fn [row] (count row))
                                (:data candidate)))))
         (map (fn [candidate]
                (assoc candidate
                  :consistent? (apply = (:counts candidate) ))))
         ;; keep the ones that have equal number of columns
         (filter :consistent?)
         ;; get rid of the 1's - there are no fields
         (filter #(not (apply = 1 (:counts %))))
         ;; get the types of each column
         (map (fn [candidate]
                (assoc candidate
                  :types (->> (util/transpose (:data candidate))
                              (map #(types/best-type-for-sequence %))))))
         ;; check to see if it has a date
         (map (fn [candidate]
                (assoc candidate
                  :has-date? (some #{'date-time} (:types candidate)))))

         ;; now pick one
         (reduce (fn [best candidate]
                   (let [least-complex (fn [best candidate]
                                         (cond
                                          (< (count (-> candidate :structure :separators))
                                             (count (-> best :sructure :separators )))
                                          candidate

                                          (and (= (count (-> candidate :structure :separators))
                                                  (count (-> best :sructure :separators )))
                                               (< (count (-> candidate :structure :enclosing))
                                                  (count (-> best :structure :enclosing))))
                                          candidate

                                          :default best))]
                     ;; dates are hard to find - if we consistantly find one in a colu8mn it is worth keeping
                     (if (not best)
                       candidate
                       (if-not (= (:has-date? best)
                                  (:has-date? candidate))
                         (some #(and (:has-date? %) %)  (list best candidate))
                         ;; both the best and the candidate either have a date or dont
                         (if-not (= (first (:counts candidate))
                                    (first (:counts best)))
                           (if (> (first (:counts candidate)) (first (:counts best))) candidate best)
                           (least-complex best candidate))))))))))
(defn smart-read
  "detects a files structure and attempts to read the file into a table"
  [_path & {:keys [max-lines] :or {max-lines 500}}]
  ;; force the path to be a string path or bail
  (let [_path (to-path _path)
        _file-structure (-> (detect-file-structure _path :max-lines 100)
                            :structure) ]
    (read-csv _path
              :enclosing (:enclosing _file-structure)
              :separators (:separators _file-structure)
              :max-lines max-lines)))

;; ----------------------------------------------------------------------
;; directory and file io helpers
;; ----------------------------------------------------------------------

(defn get-filename
  "returns the filename portion of a path "
  [path]
  (let [path (to-path path)]
    (assert (string? path))
    (.getName (java.io.File. ^String path))))

(defn get-filename-extension
  "returns the extension portion of a path"
  [path]
  (last (string/split (get-filename path) #"\.")))
(defn is-dir [path] (.isDirectory (new java.io.File ^String path)))
(defn is-file [path] (.isFile (new java.io.File ^String  path)))
(defn mkdir
  "returns path if the directory hierarchy exists. attempts to create it if it does not already exist"
  [path]
  (let [dir? (if-not (is-dir path) (.mkdirs (new java.io.File ^String path)) true)]
    (if dir? path nil)))

(defn file-exists
  "checks for existence of a file"
  [path]
  (.exists (new java.io.File ^String path)))

(defn file-last-modified
  "gets the file's last modified time"
  [path]
  (let [file (java.io.File. ^String path)]
    (when (.exists file)
      (.lastModified file))))

(defn set-file-last-modified
  "sets the file's last modified time"
  [path mtime]
  (let [file (java.io.File. ^String path)]
    (when (.exists file)
      (.setLastModified file mtime))))

(defn mv-file
  [from-path to-path]
  (.renameTo (new java.io.File ^String from-path) (new java.io.File ^String to-path)))

(defn rm-file
  "deletes the file located at path"
  [path]
  (.delete (new java.io.File ^String path)))

(defn ensure-slash
  [s]
  (if (= \/ (last s))
    s
    (str s "/")))

(defn ls
  [path]
  (map
   (fn [r]
     (let [full-path (str (ensure-slash path) r )]
       (if (is-dir full-path)
         (ensure-slash full-path)
         full-path)))
   (.list (new java.io.File ^String path))))

(def rm rm-file)

(defn rm-all
  "deletes the file or directory. will delete all files in the directory but won't recurse"
  [path]
  (if (is-dir path)
    (do (map rm-file (ls path))
        (rm-file path))
    (rm-file path)))

(defn load-file-reduce [ acc rec ]
  (if (is-dir rec)
    (reduce load-file-reduce acc (ls rec))
    (conj acc rec)))

(defn list-files-recursive
  ([base-path file-list]
     (reduce (fn [a# r#]
               (if (and (is-file r#) (not-empty (get-filename r#)))
                 (conj a# r#)
                 (list-files-recursive r# a#)))
             file-list (ls base-path)))
  ([path] (list-files-recursive path ())))

(defn file-line-count
  "returns the number of lines in a file, or 0"
  [_file]
  (with-open [r (clojure.java.io/reader (to-path _file))]
    (count (line-seq r))))

;; network helpers
;; ----------------------------------------------------------------------
(defn hostname
  "gets the result of `hostname`"
  []
  (let [result (sh "hostname")]
    (if (= 0 (:exit result))
      (string/trim-newline (:out result))
      nil)))

(defn fetch-url
  "fetch the contents of a url using a standard get request"
  [address]
  (with-open [stream (.openStream (java.net.URL. address))]
    (let  [buf (java.io.BufferedReader.
                (java.io.InputStreamReader. stream))]
      (apply str (line-seq buf)))))

(defn post-url
  "post to address with content"
  [address, content]
  (let [^java.net.HttpURLConnection cxn (.openConnection (java.net.URL. ^String address))
        ^String request-method "POST" ]
    (doto  cxn
      (.setRequestMethod ^String request-method)
      (.setDoOutput true)
      (.setDoInput true))
    (with-open [wrt (-> cxn .getOutputStream OutputStreamWriter.)]
      (.write wrt ^String content))
    (with-open [buf (-> cxn .getInputStream)]
      (apply str (line-seq buf)))))


(defn get-dir
  "returns the directory part of the filepath "
  [full-path]
  (.getParent (java.io.File. ^String full-path)))
(defn resource-path
  "given a root folder path that in the class path, returns a full path relative to the resource path"
  [folder file]
  (let [path (-> (cio/resource folder)
                      (cio/as-file)
                      (str "/" file)) ]
    path))
(defn resource-url
  [folder file]
    (let [path (-> (cio/resource folder)
                      (cio/as-url)
                      (str file)) ]
      path))

(defmacro with-read-file
  "helper for reading streaming files"
  [[stream filename] body]
  `(with-open [~stream (java.io.BufferedReader. (FileReader. ~filename))]
     ~body))

(def url-encode #(java.net.URLEncoder/encode (str %) "UTF-8"))

(def url-decode #(java.net.URLDecoder/decode (str %) "UTF-8"))


(defn str-to-bytes [s]
  (.getBytes (String. ^String s)))

(defn to-byte-array [s]
  (str-to-bytes s))

(defn str-from-bytes [in]
  (let [out (java.io.ByteArrayOutputStream. )]
    (clojure.java.io/copy in out)
    (.toString out "UTF-8")))


; encode a raw array of bytes as a base64 array of bytes
(defn encode64 [b]
  (. (new sun.misc.BASE64Encoder) encode ^bytes b))

(defn decode64 [s]
  (let [decoder (new sun.misc.BASE64Decoder)]
    (. decoder decodeBuffer ^String s)))
; decode a string encoded in base 64, result as array of bytes

; compress human readable string and return it as base64 encoded
(defn compress [s]
  (let [b (str-to-bytes s)
        output (new java.io.ByteArrayOutputStream)
        deflater (new java.util.zip.DeflaterOutputStream
                      output
                      (new java.util.zip.Deflater) 1024)]
    (. deflater write ^bytes b)
    (. deflater close)
    (str-from-bytes (encode64 (. output toByteArray)))))

(defn decompress [s]
  (let [b (decode64 s)
        input (new java.io.ByteArrayInputStream b)]
    (with-open [inflater (new java.util.zip.InflaterInputStream
                              input
                              (new java.util.zip.Inflater) 1024)]
      (str-from-bytes inflater))))


(defn vector->csv
  "given a vector of vectors, writes out a csv file"
  [v file-name]
  (with-open [^java.io.FileWriter w (mk-writer file-name)]
    (binding [*out* w]
      (doseq [row v]
        (println (clojure.string/join ", " row))))))

(defn map->csv
  "given a vector of maps, writes out a csv file"
  [m file-name]
  (vector->csv  (map vals m)
                file-name))
