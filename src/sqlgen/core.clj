(ns sqlgen.core
  (:require [honey.sql :as sql]
            [clojure.tools.macro :as m]
            [clojure.walk :as walk]
            [clojure.set :as sets])
  (:gen-class))

(defn inform [prefix val]
  (binding [*out* *err*]
    (println (str prefix " " val)))
  val)

(defn get-selection-cols [query]
  (let [cols (->> (query :select)
                  (map (fn [col] (if (vector? col) (second col) col))))]
    ;; if the selection contains *, then we don't need to also specify any other cols
    (if (some #{:*} cols) [:*] cols)))

(defn merge-using-with [query fragment]
  (let [find-and-bump (fn [with] (-> with last first name (subs 1) Integer/parseInt inc (->> (str "q")) keyword))
        base (if (not (contains? query :with))
               {:with [[:q1 query]] :select (get-selection-cols query) :from :q1}
               (let [with (:with query)
                     name (find-and-bump with)
                     remain (dissoc query :with)]
                 {:with (conj with [name remain])
                  :select (get-selection-cols query) :from name}))]
    (merge base fragment)))

(defn precedence-merge [query fragment]
  (let [precedence (zipmap [:from :where :group-by :having :select :order-by :limit] (range))]
    (letfn [(prec [a]
              (if (empty? a)
                0
                (->> (keys a)
                     (map precedence)
                     (remove nil?) ;necessary for e.g. inner-join
                     (apply max))))
            (higher-precedence [a b]
              (<= (prec a) (prec b)))]
      ((if (higher-precedence fragment query) merge-using-with merge) query fragment))))

(defn symbol->keyword [form] (if (symbol? form) (keyword form) form))

(defn keyword-or-alias [form]
  (cond (symbol? form) (keyword form)
        (list? form) [(keyword (first form)) (keyword (nth form 2))]  ;:as should be 2nd
        :else form))

;;; this is used by named-expr and inside e.g. mutate to avoid
;;; infinite recursion. when a named expression refers to its own name
;;; it refers to an existing column, not itself.
(defn replace-with-keyword-deep [needle form]
  (walk/postwalk (fn [x] (if (= x needle) (keyword (name needle)) x))
                 form))

(defn named-expr
  ([pair]
   (let [col (first pair)
         expr (second pair)]
     [(replace-with-keyword-deep col expr) (keyword col)]))
  ([over-params pair]
   (let [col (first pair)
         expr (second pair)]
     [(replace-with-keyword-deep col expr) over-params (keyword col)])))

;;; needed as order-by has the :asc :desc syntax that is inconsistent
(defn order-by-wrap-if-needed [expr]
  (if (and (vector? expr)
           (not= (last expr) :desc))
    [expr]
    expr))


(defmacro expand-expr [expr]
  `(m/macrolet [(~'and [& args#] `[:and ~@(map symbol->keyword args#)])
                (~'or [& args#] `[:or ~@(map symbol->keyword args#)])
                (~'= [a# b#] `[:= ~(symbol->keyword a#) ~(symbol->keyword b#)])
                (~'not= [a# b#] `[:<> ~(symbol->keyword a#) ~(symbol->keyword b#)])
                (~'> [a# b#] `[:> ~(symbol->keyword a#) ~(symbol->keyword b#)])
                (~'>= [a# b#] `[:>= ~(symbol->keyword a#) ~(symbol->keyword b#)])
                (~'< [a# b#] `[:< ~(symbol->keyword a#) ~(symbol->keyword b#)])
                (~'<= [a# b#] `[:<= ~(symbol->keyword a#) ~(symbol->keyword b#)])
                (~'like [a# b#] `[:like ~(symbol->keyword a#) ~(symbol->keyword b#)])
                (~'not [a#] `[:not ~(symbol->keyword a#)])
                (~'if-else [cond# b1# b2#] `[:if ~(symbol->keyword cond#) ~(symbol->keyword b1#) ~(symbol->keyword b2#)])
                ;; TODO: this in only supports literal lists. just inner join instead. not in might be useful
                (~'in [expr# & vals#] `[:in ~(symbol->keyword expr#) [:composite ~@(map symbol->keyword vals#)]])
                ;; json
                (~'json-extract [from# path#] `[:json_extract ~(symbol->keyword from#) ~path#])
                (~'json-extract-scalar [from# path#] `[:json_extract_scalar ~(symbol->keyword from#) ~path#])
                (~'json-size [from# path#] `[:json_size ~(symbol->keyword from#) ~path#])
                (~'json-parse [a#] `[:json_parse ~(symbol->keyword a#)])
                (~'json-format [a#] `[:json_format ~(symbol->keyword a#)])
                (~'is-json-scalar [a#] `[:is_json_scalar ~(symbol->keyword a#)])
                ;; arrays
                ;; maps
                (~'map-keys [a#] `[:map_keys ~(symbol->keyword a#)])
                (~'map-values [a#] `[:map_values ~(symbol->keyword a#)])
                ;; strings
                (~'s-concat [& args#] `[:concat ~@(map symbol->keyword args#)])
                (~'length [a#] `[:length ~(symbol->keyword a#)])
                (~'ltrim [a#] `[:ltrim ~(symbol->keyword a#)])
                (~'rtrim [a#] `[:rtrim ~(symbol->keyword a#)])
                (~'trim [a#] `[:trim ~(symbol->keyword a#)])
                (~'split-part [string# delim# idx#] `[:split_part ~(symbol->keyword string#) ~delim# ~idx#])
                ;; misc
                (~'approx-distinct [a#] `[:approx_distinct ~(symbol->keyword a#)])
                (~'rand [] [:rand])
                (~'count [] [:count :*])
                ;; TODO: bug: and and or do not work as we don't expand
                (~'count-if [a#] `[:count_if ~(symbol->keyword a#)])
                (~'count-distinct [arg#] `[:count [:distinct ~(symbol->keyword arg#)]])
                (~'sum [a#] `[:sum ~(symbol->keyword a#)])
                (~'min [a#] `[:min ~(symbol->keyword a#)])
                (~'min-by [a# sorter#] `[:min_by ~(symbol->keyword a#) ~(symbol->keyword sorter#)])
                (~'max [a#] `[:max ~(symbol->keyword a#)])
                (~'max-by [a# sorter#] `[:max_by ~(symbol->keyword a#) ~(symbol->keyword sorter#)])
                (~'avg [a#] `[:avg ~(symbol->keyword a#)])
                (~'coalesce [& args#] `[:coalesce ~@(map symbol->keyword args#)])
                (~'date [arg#] `[:date ~(symbol->keyword arg#)])
                (~'from-unixtime [arg#] `[:from_unixtime ~(symbol->keyword arg#)])
                (~'cast [arg# type#] `[:cast ~(symbol->keyword arg#) ~(keyword type#)])
                (~'case-when [& args#] `[:case ~@(map symbol->keyword args#)])
                (~'+ [a# b#] `[:+ ~(symbol->keyword a#) ~(symbol->keyword b#)])
                (~'- [a# b#] `[:- ~(symbol->keyword a#) ~(symbol->keyword b#)])
                (~'* [a# b#] `[:* ~(symbol->keyword a#) ~(symbol->keyword b#)])
                (~'/ [a# b#] `[:/ ~(symbol->keyword a#) ~(symbol->keyword b#)])]
               ~expr))

(defmacro table [tbl]
  `{:from [~(keyword tbl)]
    :select [:*]})

;;; TODO: dropping a column would be particularly helpful
(defmacro select [ds & exprs]
  `(precedence-merge ~ds {:select [~@(map keyword-or-alias exprs)]}))

(defmacro where [ds & exprs]
  `(precedence-merge ~ds {:where (expand-expr (~'and ~@exprs))}))

(defmacro order-by [ds & exprs]
  `(m/macrolet [(~'desc [arg#] `[~(symbol->keyword arg#) :desc])]
               (precedence-merge ~ds {:order-by (mapv order-by-wrap-if-needed
                                                      (expand-expr [~@(mapv symbol->keyword exprs)]))})))

;;; TODO: unify mutate, mutate-grouped, and summarize. They are all
;;; just defining selection expressions that are named, maybe with
;;; over, maybe clobbering.
(defmacro mutate [ds & forms]
  (let [pairs (partition 2 forms)
        new-cols (m/mexpand-all `(m/symbol-macrolet [~@(->> pairs
                                                            (map (fn [[x y]]
                                                                   [x (replace-with-keyword-deep x y)]))
                                                            (apply concat))]
                                                    ~(mapv named-expr pairs)))]
    `(precedence-merge ~ds {:select (concat (get-selection-cols ~ds)
                                            (expand-expr ~new-cols))})))

(defmacro mutate-grouped [ds groups & forms]
  (let [pairs (partition 2 forms)
        over-params (if (empty? groups)
                      {}
                      {:partition-by (mapv symbol->keyword groups)})
        new-cols (m/mexpand-all `(m/symbol-macrolet [~@(->> pairs
                                                            (map (fn [[x y]]
                                                                   [x (replace-with-keyword-deep x y)]))
                                                            (apply concat))]
                                                    [[[:over ~@(mapv (partial named-expr over-params)
                                                                     pairs)]]]))]
    `(precedence-merge ~ds {:select (concat (get-selection-cols ~ds)
                                            (expand-expr ~new-cols))})))

(defn limit [ds limit]
  (precedence-merge ds {:limit limit}))

(def slice-head limit)

(defn slice-sample [ds n]
  (-> ds
      (order-by (rand))
      (slice-head n)))

(defmacro summarize [ds & forms]
  (let [pairs (partition 2 forms)
        replace-form `{:select (m/symbol-macrolet [~@(->> pairs
                                                          (map (fn [[x y]]
                                                                 [x (replace-with-keyword-deep x y)]))
                                                          (apply concat))]
                                                  ~(mapv named-expr pairs))}]
    ;; we use precedence-merge to end up wrapping the query as the
    ;; select may have been mutated
    `(precedence-merge ~ds (expand-expr ~(m/mexpand-all replace-form)))))

(defn rewrite-for-group [groups form]
  (if (list? form)
    (let [[f & args :as whole] form]
      (cond (= f 'mutate) `(mutate-grouped ~groups ~@args)
            :else whole))
    form))

(defn contains-summarize [forms]
  (->> forms
       (map first)
       (some #{'summarize})))

;;; TODO: order by - should influence the order by on the window function
;;; TODO: support slice functions for groups
;;; TODO: allow creating cols to group on
(defmacro group [ds groups & forms]
  `(-> ~ds
       ;; operation
       ~@(map (partial rewrite-for-group groups) forms)
       ;; update the select
       ~(if (contains-summarize forms)
          `(update-in [:select] (fn [a# b#] (concat b# a#)) ~(mapv symbol->keyword groups))
          `(identity))
       ;; add the group by
       ~(if (contains-summarize forms)
          `(merge {:group-by ~(mapv symbol->keyword groups)})
          `(identity))))

(defmacro count-by [ds & groups]
  `(-> ~ds
       (group [~@groups]
              (~'summarize ~'n (~'count)))
       (order-by (~'desc ~'n))))

;;; TODO: would be great to allow more than just =. the macros should
;;; eval/expand a list of expressions and wrap in and. any lone keywords
;;; should expand to (= kw kw) first. how to get the selection cols??
(defn join [join-type-kw query1 query2 join-cols suffix]
  (let [q1-cols (get-selection-cols query1)
        q2-cols (get-selection-cols query2)
        scope-col (fn [table col] (keyword (format "%s.%s" (name table) (name col))))
        rename-col (fn [potential-clashes table col]
                     [(scope-col table col)
                      (if ((set potential-clashes) col)
                        (keyword (format "%s%s" (name col) suffix))
                        col)])]
    {:select (vec (concat
                   (mapv (partial rename-col [] :jq1) q1-cols)
                   (->> q2-cols
                        (remove (set (if (empty? join-cols) q1-cols join-cols))) ; used to maintain order
                        (mapv (partial rename-col q1-cols :jq2)))))
     :from [[query1 :jq1]]
     join-type-kw [[query2 :jq2]
                   (->> (if-not (empty? join-cols)
                          join-cols
                          (sets/intersection (set q1-cols) (set q2-cols)))
                        (mapv (fn [col] [:= (scope-col :jq1 col) (scope-col :jq2 col)]))
                        (into [:and])
                        (inform "join using"))]}))

(defmacro inner-join [query1 query2 & {:keys [using suffix]}]
  (let [q1 (m/mexpand-all query1)
        q2 (m/mexpand-all query2)]
    `(join :inner-join ~q1 ~q2 ~(mapv symbol->keyword using) ~(or suffix ""))))

(defmacro left-join [query1 query2 & {:keys [using suffix]}]
  (let [q1 (m/mexpand-all query1)
        q2 (m/mexpand-all query2)]
    `(join :left-join ~q1 ~q2 ~(mapv symbol->keyword using) ~(or suffix ""))))

;;; we select * from the union so that the query has a select
;;; statement and can be precedence-merged
(defn union [& queries]
  {:select [:*] :from [{:union queries}]})

(defn union-all [& queries]
  {:select [:*] :from [{:union-all queries}]})

(defmacro unnest-longer [ds & form]
  (let [[nested-col unnested-vars]
        (cond (and (= (count form) 3) (= (second form) :as))
              (let [[nested-col _ new-unnested-var] form]
                [(keyword nested-col) [(keyword new-unnested-var)]])
              (and (= (count form) 4) (= (second form) :as))
              (let [[nested-col _ new-unnested-key-var new-unnested-vale-var] form]
                [(keyword nested-col) (mapv keyword [new-unnested-key-var new-unnested-vale-var])])
              :else (throw (Exception. "Need to provide an :as form to unnest-longer")))]
    `{:select (vec (concat (remove #{~nested-col} (get-selection-cols ~ds))
                           ~unnested-vars))
      :from [~ds]
      :cross-join [[[:raw ["UNNEST(" ~(name nested-col) ") AS t ("
                           ~(clojure.string/join "," (map name unnested-vars)) ")"]]]]}))

(defn -main [& args]
  (binding [*ns* (find-ns 'sqlgen.core)]
    (-> (read)
        eval
        (->> (inform "data structure is"))
        (sql/format :inline true)
        (get 0)
        (->> (inform "query is"))
        println)))

;;; TODO: unnesting arrays and maps unnest-longer. can we do wider?


;;; some design notes:


;;; diff between summarise and select and mutate:

;;; select is about preds to match existing stuff. it replaces cols,
;;; allows renaming with :as, but does not allow creating cols from
;;; new expressions.

;;; summarise replaces all cols, it has a different form to make it
;;; convenient to create a new set of columns from expressions. most
;;; useful for, well, summarising.

;;; mutate adds to the existing selection with a similar form to
;;; summarize.

;;; summarize and mutate allow referring to just created columns. they
;;; will resubstitute expressions to make this possible. using another
;;; summarize or mutate avoids this at the cost of extra nesting.

;;; currently there is no transmute because I can't decide on a form
;;; that feels consistent. you can (summarize a a) however.


;;; how is this different from dbplyr? don't need a dbi connection.
;;; but mostly it breaks with trying to have exactly the same
;;; semantics as dplyr. it is just a bit closer to sql which makes the
;;; generated output more predictable and understandable.
