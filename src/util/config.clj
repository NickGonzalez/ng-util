(ns util.config
  "A very simple configuration system based on the core concept of overriding dynamic variables defined in your code using a config file.
  Basic usage is as follows:
  1. Run (export-config ..) with a list of namespaces and an output path to generate the default configuration file for your project.
  2. Modify the values in the newly created config file.
  3. On startup, call (set-config! ...) with the path of the modified config file BEFORE any of the redefined variables are used.
  4. Notice that the variables in your project have been properly set to the values in the config file. "
  (:require [util.io :as io]))

(defonce config-data (atom {}))

(def ^:dynamic *config-dir* "config/")
(def ^:dynamic *config-template* (str *config-dir* "template.clj"))
(def ^:dynamic *default-config* (str *config-dir* "settings.clj"))


(defn dynamic?
  "takes a var v and returns true if it's dynamic"
  [v]
  (:dynamic (meta v) false))

(defn list-dynamic-vars
  "takes in a list of publics with metas and returns the same list with only dynamic vars included"
  [ns]
  (filter (comp dynamic? second) (ns-publics ns) ))

(defn dynamic-var-map
  "given a namespace returns a map of dynamic vars to values"
  [ns]
  (apply merge (map (fn [[k v]]
                {k  (deref v)})
                    (list-dynamic-vars ns))))

(defn bindings-map
  "takes a dynamic var map and returns a map suitable to be used in a with-bindings call"
  [ns vm]
  (apply merge (map (fn [[k v]]
                      {(ns-resolve ns k) v})
                    vm)))

(defn generate-config
  [ns]
  {:namespace ns
   :overrides (dynamic-var-map ns)})

(defn generate-configs
  [ns-list]
  (map generate-config ns-list))

(defn merged-bindings-map
  "takes a list of config objects and converts it into a bindings map"
  [config-list]
  (apply merge
         (map  #(bindings-map (:namespace %)
                              (:overrides %))
               config-list)))
(defmacro with-configs
  [configs & body]
  `(if (empty? ~configs)
     (do ~@body)
     (with-bindings (merged-bindings-map ~configs)
       (do ~@body))))

(defmacro with-config
  [ & body]
  `(if (empty? @config-data)
     (do ~@body)
       (with-bindings (merged-bindings-map @config-data)
         (do ~@body))))

(defn write-config
  [data config-path]
  (let [out-str  (with-out-str
                   (clojure.pprint/pprint data))]
    (spit config-path out-str)))

(defn export-config
  "exports a single config for a list of namespaces into a file named by config-path"
  ([ns-list config-path]
     (write-config (generate-configs ns-list) config-path))
  ([ns-list] (export-config ns-list *config-template*)))

(defn setvar!
  [ns var-name value]
  (alter-var-root (ns-resolve ns var-name) (fn [_] value)))

(defn alter-default-config
  "imports config and alter default values in code"
  [ccdata]
  (doall (map
    (fn [row]
      (doall (map
              (fn [[k v]]
                (try
                  (setvar! (:namespace row) k v)
                  (catch Throwable e
                    (println (str  "Invalid Config Setting: " (:namespace row)
                                       " key: "  k " :value " v " . ")))))
        (:overrides row))))
    ccdata)))

(defn import-config
  "imports config file formally exported with export-config"
  [config-path]
  (io/deserialize config-path))


(defn find-section
  [ns config]
  (some #(and (= (:namespace %)) %) config))


(defn override-config
  [config overrides]
  (reduce (fn [result section ]
            (conj result
                  (or ) (find-section (:namespace section) config)
                  ))
          config)
  (doseq [{ns :namespace val-map :overrides :as obj}  overrides]
    (conj (remove #(= ns (:namespace % :missing))
                  config)
          (merge (find-section ns config)
                 (or val-map {})))))


(defn set-config!
  "imports a config file and sets the config data"
  [path & {:keys [overrides] :or {overrides '()}}]
  (cond
   (string? path) (when-let [config (import-config path)]
                      (alter-default-config config)
                      (reset! config-data config))
   (seq path) (let [config path]
                (alter-default-config config)
                (reset! config-data config))))

(defn set-default-config!
  "imports the default config file and sets the config data"
  []
  (set-config! *default-config*))

(defn update-overrides-section
  [template config]
  (merge template  (select-keys config (keys template))))

(defn update-section
  [{to :overrides tn :namespace :as template}
   {co :overrides cn :namespace :as config} ]
  (cond
   (= tn cn) {:namespace tn
              :overrides (update-overrides-section to co)}
   :else template))


(defn update-config-data
  "returns an updated config using all of the keys in template overriding them with the keys from config that are still valid"
  [template config]
  ;; first update the namespace list
  (map  #(update-section % (find-section (:namespace %) config))
        template))

(defn update-config-file
  "returns an updated config using all of the keys in template overriding them with the keys from config that are still valid"
  [template config-path]
  ;; first update the namespace list
  (update-config-data template (import-config config-path)))

(defn load-template []
  (import-config *config-template*))

(defn list-config-files
  [& {:keys [dir] :or {dir *config-dir*}}]
  (filter (partial  re-find #".*\.clj")
          (remove #{*config-template*}
                  (util.io/ls dir))))

(defn modify-config-data
  [config-data update-section-fn]
  (map update-section-fn config-data))

(defn has-section? [config-data ns]
  (some #(= (:namespace %) ns) config-data))

(defn add-section [config-data ns]
  (if (has-section? config-data ns)
    config-data
    (conj config-data {:namespace ns
                       :overrides {}})))

(defn update-all-config-files
  [update-fn & {:keys [config-path]
                :or {config-path *config-dir*}}]
  (map #(write-config (update-fn (import-config %))
                      %)
       (list-config-files :config-path config-path)))


(defn get-keys [config-data key-list]
  (reduce (fn [acc section ]
            (merge acc (select-keys (:overrides section {}) key-list)))
          {}
          config-data))
(comment
  (defn update-app-id [config-data]
    (let [ {aid '*app-id* as '*app-secret*} (get-keys config-data '[*app-id* *app-secret*])]
      (println (str  "aid " aid " as " as))
      (modify-config-data (add-section config-data 'server.app)
                          #(if-not (= (:namespace %) 'server.app)
                             (assoc %
                               :overrides (dissoc (:overrides  % {})
                                                  '*app-id* '*app-secret*))
                             (assoc %
                               :overrides (assoc (:overrides % {})
                                            '*app-id* aid
                                            '*app-secret* as)))))))
