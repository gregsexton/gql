(ns sqlgen.core
  (:require [honey.sql :as sql]
            [clojure.tools.macro :as m]
            [clojure.walk :as walk])
  (:gen-class))

(defmacro select [ds & exprs]
  `(merge ~ds {:select [~@(map keyword exprs)]}))

(defn keywordize [form] (if (symbol? form) (keyword form) form))

(defmacro expand-expr [expr]
  `(m/macrolet [(~'and [& args#] `[:and ~@(map keywordize args#)])
                (~'or [& args#] `[:and ~@(map keywordize args#)])
                (~'= [a# b#] `[:= ~(keywordize a#) ~(keywordize b#)])
                (~'json-extract-scalar [from# path#]
                 `[[:json_extract_scalar ~(keywordize from#) ~path#]])]
               ~expr))

(defmacro where [ds expr]
  `(merge ~ds {:where (expand-expr ~expr)}))

;;; i can't have keyword symbols eval expand-expr otherwise the symbols will be evaluated. or I use quoting but macrolet is smart enough to not expand that
;;; i can't have expand-expr evaluate keyword symbols because all the symbols I'm hoping to expand will be keywordised
;;; I have to have expand-expr as it is walking down keywordise specific things, which is probably the safer thing to do anyway

(expand-expr (json-extract-scalar 5 4))
(expand-expr (json-extract-scalar foo 4))
(expand-expr (= 5 4))
(expand-expr (= foo 4))
(where {} (= (json-extract-scalar 'data "$.topic")
             "backup"))
(where {} (= (json-extract-scalar data "$.topic")
             another-col))

(defmacro table [tbl]
  `{:from [~(keyword tbl)]})

(-> (table src_wa_fastdesk_tickets)
    (select data)
    (where (and (= ds "<DATEID>")
                flag_col
                (= (json-extract-scalar data "$.topic")
                   "backup")))
    (sql/format))

;;; how to represent the table metadata and pass that along?
