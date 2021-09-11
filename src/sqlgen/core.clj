(ns sqlgen.core
  (:require [honey.sql :as sql]
            [clojure.tools.macro :as m]
            [clojure.walk :as walk])
  (:gen-class))

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
                (~'json-extract-scalar [from# path#]
                 `[[:json_extract_scalar ~(keywordize from#) ~path#]])]
               ~expr))

(defmacro table [tbl]
  `{:from [~(keyword tbl)]})

(defmacro select [ds & exprs]
  `(merge ~ds {:select [~@(map keyword-or-alias exprs)]}))

(defmacro where [ds & exprs]
  `(merge ~ds {:where (expand-expr (~'and ~@exprs))}))

;;; allowing referencing just declared vars by substituting. what's
;;; the tradeoff compared to nesting? is an engine smart enough not to
;;; duplicate effort?
(defmacro mutate [ds & forms]
  (let [pairs (partition 2 forms)
        update-form `(update-in ~ds [:select] concat
                                (m/symbol-macrolet [~@forms]
                                                   ~(mapv named-expr pairs)))]
    `(expand-expr ~(m/mexpand-all update-form))))

;;; TODO: precedence - you usually can't refer to things you've added to select, so will have to be careful around mutate
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
    (mutate topic (json-extract-scalar data "$.topic")
            status (json-extract-scalar data "$.status")
            is-closed (= status "closed")) ;note: referring to status
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
