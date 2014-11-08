(ns util.core-test
  (:require [clojure.test :refer :all]
            [util.io :as io]
            [util.util :as util]
            [util.types :as types]))

(deftest test-utils []
  (is (= (util/not-nil? nil) false))
  (is (= (util/not-nil? true) true))
  (is (= 15 (count (util/ensure-sizev [1 2 3] 15)))))
