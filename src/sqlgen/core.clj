(ns sqlgen.core
  (:require [honey.sql :as sql]
            [clojure.tools.macro :as m]
            [clojure.walk :as walk])
  (:gen-class))

(defn get-selection-cols [query]
  (->> (query :select)
       (map (fn [col] (if (vector? col)
                        (second col)
                        col)))))

(defn precedence-merge [query fragment]
  (let [precedence (zipmap [:from :where :group-by :having :select :order-by :limit] (range))]
    (letfn [(prec [a]
              (if (empty? a)
                0
                (->> (keys a)
                     (map precedence)
                     (apply max))))
            (higher-precedence [a b]
              (<= (prec a) (prec b)))]
      (if (higher-precedence fragment query)
        (merge {:select (get-selection-cols query) :from query} fragment)
        (merge query fragment)))))

(defn keywordize [form] (if (symbol? form) (keyword form) form))

(defn keyword-or-alias [form]
  (cond (symbol? form) (keyword form)
        (list? form) [(keyword (first form)) (keyword (nth form 2))]  ;:as should be 2nd
        :else form))

(defn named-expr [pair]
  (let [col (first pair)
        expr (second pair)]
    [expr (keyword col)]))

(defmacro expand-expr [expr]
  `(m/macrolet [(~'and [& args#] `[:and ~@(map keywordize args#)])
                (~'or [& args#] `[:or ~@(map keywordize args#)])
                (~'= [a# b#] `[:= ~(keywordize a#) ~(keywordize b#)])
                (~'not= [a# b#] `[:<> ~(keywordize a#) ~(keywordize b#)])
                (~'not [a#] `[:not ~(keywordize a#)])
                (~'json-extract-scalar [from# path#]
                 `[[:json_extract_scalar ~(keywordize from#) ~path#]])]
               ~expr))

(defmacro table [tbl]
  `{:from [~(keyword tbl)]})

(defmacro select [ds & exprs]
  `(precedence-merge ~ds {:select [~@(map keyword-or-alias exprs)]}))

(defmacro where [ds & exprs]
  `(precedence-merge ~ds {:where (expand-expr (~'and ~@exprs))}))

;;; allowing referencing just declared vars by substituting. what's
;;; the tradeoff compared to nesting? is an engine smart enough not to
;;; duplicate effort?
;;; nesting might prevent effort dup and it also takes care of using
;;; multipe mutates and referring to vars, there might be a _lot_ of
;;; nesting though.
(defmacro mutate [ds & forms]
  (let [pairs (partition 2 forms)
        update-form `(update-in ~ds [:select] concat
                                (m/symbol-macrolet [~@forms]
                                                   ~(mapv named-expr pairs)))]
    `(expand-expr ~(m/mexpand-all update-form))))

;;; TODO: grouping
;;; TODO: joins
;;; TODO: allow using variables - probably need to use binding?
(-> (table src_wa_fastdesk_tickets)
    (where (= ds "<DATEID>")
           (or flag-col
               (= (json-extract-scalar data "$.topic")
                  "backup"))
           (not= (json-extract-scalar data "$.status") nil))
    (select data (id :as ticket-id))
    (where (= id "1234"))               ;this demonstrates precedence
    (mutate topic (json-extract-scalar data "$.topic")
            status (json-extract-scalar data "$.status")
            is-closed (= status "closed")) ;note: referring to status
    (mutate not-is-closed (not is-closed))  ;TODO: this does not work currently
    (sql/format :inline true))

;;; how to represent the table metadata and pass that along?

(mutate {:select []}
        status (json-extract-scalar data "$.status")
        is-closed (= status "closed"))

(expand-expr (json-extract-scalar 5 4))
(expand-expr (json-extract-scalar foo 4))
(expand-expr (= 5 4))
(expand-expr (= foo 4))
(where {} (= (json-extract-scalar 'data "$.topic")
             "backup"))
(where {} (= (json-extract-scalar data "$.topic")
             another-col))
