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
                (~'> [a# b#] `[:> ~(keywordize a#) ~(keywordize b#)])
                (~'not [a#] `[:not ~(keywordize a#)])
                (~'json-extract-scalar [from# path#]
                 `[[:json_extract_scalar ~(keywordize from#) ~path#]])
                (~'approx-distinct [a#]
                 `[[:approx_distinct ~(keywordize a#)]])
                (~'rand [] [[:rand]])
                (~'count [] [[:count :*]])
                (~'count-if [a#] `[[:count_if ~(keywordize a#)]])
                (~'sum [a#] `[[:sum ~(keywordize a#)]])
                (~'/ [a# b#] `[:/ ~(keywordize a#) ~(keywordize b#)])]
               ~expr))

;;; TODO: should always have a select and should lookup all the cols
;;; TODO: allow specifying partitions? so we get the base with a where ds straight away?
(defmacro table [tbl]
  `{:from [~(keyword tbl)]
    :select [:*]})

(defmacro select [ds & exprs]
  `(precedence-merge ~ds {:select [~@(map keyword-or-alias exprs)]}))

(defmacro where [ds & exprs]
  `(precedence-merge ~ds {:where (expand-expr (~'and ~@exprs))}))

(defmacro order-by [ds & exprs]
  `(m/macrolet [(~'desc [arg#] `[~(keywordize arg#) :desc])]
               (precedence-merge ~ds {:order-by (expand-expr [~@(map keywordize exprs)])})))

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

(defn limit [ds limit]
  (precedence-merge ds {:limit limit}))

;;; TODO: naming columns the same as functions causes an infinite loop e.g. sum (sum foo)
;;; TODO: would be cool to auto-name, probably need to change the form for this
(defmacro summarize [ds & forms]
  (let [pairs (partition 2 forms)
        replace-form `(assoc-in ~ds [:select]
                                (m/symbol-macrolet [~@forms]
                                                   ~(mapv named-expr pairs)))]
    `(expand-expr ~(m/mexpand-all replace-form))))

;;; diff between summarise and select and mutate: select is about
;;; preds to match existing stuff. summarise is a different form to
;;; name a column with an agg function. could possibly use mutate but
;;; this will not work well under groups. mutate adds to the existing
;;; but summarize replaces

;;; TODO: inner join - should auto select vars and print to stderr what we're using. should allow choosing the vars
;;; TODO: left join
;;; TODO: would be great to allow more than just =

;;; TODO: slice sample and other slices
;;; TODO: select preds e.g. not this, starts-with etc
;;; TODO: major clean up
;;; TODO: wrap up a read-eval-print loop from stdin with pretty print option
;;; TODO: with

;;; TODO: grouping -- filter/where - having?
;;; TODO: grouping -- mutate - window funcs
;;; TODO: grouping -- order by - probably should influence the order by on the window function?
                                        ; TODO grouping -- slice functions

;;; TODO: allow using variables - probably need to use binding?
                                        ; just don't? consider out of scope

(-> (table src_wa_fastdesk_tickets)
    (where (= ds "<DATEID>")
           (or flag-col
               (= (json-extract-scalar data "$.topic")
                  "backup"))
           (not= (json-extract-scalar data "$.status") nil))
    (select data (id :as ticket-id))
    (where (= id "1234"))               ;this demonstrates precedence
    (where flag-col)               ;this demonstrates just using a col
    (mutate topic (json-extract-scalar data "$.topic")
            status (json-extract-scalar data "$.status")
            is-closed (= status "closed")) ;note: referring to status
    (mutate not-is-closed (not is-closed))  ;TODO: this does not work currently
    (order-by is-closed
              (desc status)
              (rand)
              (desc (json-extract-scalar data "$.topic")))
    (limit 100)
    (sql/format :inline true :pretty true)
    println)

;;; groups should basically just call existing funcs with a group-by
;;; tacked on ignoring precedence and the group names in the select -
;;; I think

;;; TODO: allow creating cols to group on?
(defmacro group [ds groups & forms]
  `(-> ~ds
       ~@forms
       (update-in [:select] (fn [a# b#] (concat b# a#)) ~(mapv keywordize groups))
       (merge {:group-by ~(mapv keywordize groups)})))

(defmacro count-groups [ds & groups]
  `(-> ~ds
       (group [~@groups]
              (summarize ~'n (~'count)))
       (order-by (~'desc ~'n))))

(count-groups {:select [:foo :bar]}
              foo)

(sql/format (group {:select [:foo :bar]}
                   [a b]
                   (summarize n (count))))

(-> (table src_wa_fastdesk_tickets)
    (where (= ds "<DATEID>")
           (or flag-col
               (= (json-extract-scalar data "$.topic")
                  "backup"))
           (not= (json-extract-scalar data "$.status") nil))
    (mutate topic (json-extract-scalar data "$.topic")
            status (json-extract-scalar data "$.status")
            is-closed (= status "closed"))
    (group [topic status]
           (summarize n (count)
                      n_closed (count-if is-closed)))
    (sql/format :inline true :pretty true)
    (get 0)
    println)

(-> (table src_wa_fastdesk_tickets)
    (where (= ds "<DATEID-2>"))
    (mutate topic (json-extract-scalar data "$.topic"))
    (group [topic] (summarize n (count)))
    (where (> n 100))
    (summarize n (count))
    (sql/format :inline true))

(-> (table src_wa_fastdesk_tickets)
    (where (= ds "<DATEID-2>"))
    (mutate topic (json-extract-scalar data "$.topic"))
    (count-groups topic)
    (sql/format :inline true))

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

;;; TODO: need to allow selecting join criteria - a subset at least - same as where: wrap in and
;;; TODO: need to allow suffixing dup cols

(defn- join [join-type-kw query1 query2]
  (let [q1-cols (get-selection-cols query1)
        q2-cols (get-selection-cols query2)
        scope-col (fn [table col] (keyword (format "%s.%s" (name table) (name col))))]
    {:select (concat
              (mapv (partial scope-col :q1) q1-cols)
              (->> q2-cols
                   (remove (set q1-cols)) ; used to maintain order
                   (mapv (partial scope-col :q2))))
     :from [[query1 :q1]]
     join-type-kw [[query2 :q2]
                   (->> (clojure.set/intersection (set q1-cols) (set q2-cols))
                        (map (fn [col] [:= (scope-col :q1 col) (scope-col :q2 col)]))
                        (into [:and]))]}))

(def inner-join (partial join :inner-join))
(def left-join (partial join :left-join))

(-> (inner-join (-> (table src_wa_fastdesk_tickets)
                    (select a b c))
                (-> (table src_wa_fastdesk_tickets)
                    (select a d c)))
    (sql/format :inline true))

(let [counts (-> (table src_wa_fastdesk_tickets)
                 (group [topic] (summarize n (count))))]
  (-> (table src_wa_fastdesk_tickets)
      (where (> ds "<DATEID-2>"))
      (select topic data)
      (inner-join counts)
      (sql/format :inline true)))             ;this is how you break queries out and organise them

(-> (table src_wa_fastdesk_tickets)
    (where (> ds "<DATEID-2>"))
    (select topic data)
    (inner-join (-> (table src_wa_fastdesk_tickets)
                    (group [topic] (summarize n (count)))))   ;this is inlining
    (sql/format :inline true))

(-> (table src_wa_fastdesk_tickets)
    (where (> ds "<DATEID-2>"))
    (select topic data)
    (inner-join (table foo))           ;this is how you join with an existing table
    (sql/format :inline true))
