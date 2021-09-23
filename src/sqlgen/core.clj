(ns sqlgen.core
  (:require [honey.sql :as sql]
            [clojure.tools.macro :as m]
            [clojure.walk :as walk]
            [clojure.set :as sets])
  (:gen-class))


;;; TODO: slice sample and other slices
;;; TODO: select preds e.g. not this, starts-with etc

;;; TODO: joins: would be great to allow more than just =

;;; TODO: grouping -- filter/where - having?
;;; TODO: grouping -- mutate - window funcs - should allow over() with no partition by
;;; TODO: grouping -- order by - probably should influence the order by on the window function?
;;; TODO: grouping -- slice functions


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
                     (remove nil?) ;necessary for e.g. inner-join
                     (apply max))))
            (higher-precedence [a b]
              (<= (prec a) (prec b)))]
      (if (higher-precedence fragment query)
        ;; TODO: honeysql does support with, could flatten the nesting using this
        (merge {:select (get-selection-cols query) :from query} fragment)
        (merge query fragment)))))

(defn keywordize [form] (if (symbol? form) (keyword form) form))

(defn keyword-or-alias [form]
  (cond (symbol? form) (keyword form)
        (list? form) [(keyword (first form)) (keyword (nth form 2))]  ;:as should be 2nd
        :else form))

(defn named-expr
  ([pair]
   (let [col (first pair)
         expr (second pair)]
     [expr (keyword col)]))
  ([over-params pair]
   (let [col (first pair)
         expr (second pair)]
     [expr over-params (keyword col)])))

;;;TODO: cast, case
(defmacro expand-expr [expr]
  `(m/macrolet [(~'and [& args#] `[:and ~@(map keywordize args#)])
                (~'or [& args#] `[:or ~@(map keywordize args#)])
                (~'= [a# b#] `[:= ~(keywordize a#) ~(keywordize b#)])
                (~'not= [a# b#] `[:<> ~(keywordize a#) ~(keywordize b#)])
                (~'> [a# b#] `[:> ~(keywordize a#) ~(keywordize b#)])
                (~'>= [a# b#] `[:>= ~(keywordize a#) ~(keywordize b#)])
                (~'< [a# b#] `[:< ~(keywordize a#) ~(keywordize b#)])
                (~'<= [a# b#] `[:<= ~(keywordize a#) ~(keywordize b#)])
                (~'like [a# b#] `[:like ~(keywordize a#) ~(keywordize b#)])
                (~'not [a#] `[:not ~(keywordize a#)])
                (~'if-else [cond# b1# b2#] `[:if ~(keywordize cond#) ~(keywordize b1#) ~(keywordize b2#)])
                (~'in [expr# & vals#] `[:in ~(keywordize expr#) [:composite ~@(map keywordize vals#)]])
                ;; TODO: this in only supports literal lists. for testing against sub queries could introduce semi-join
                (~'json-extract-scalar [from# path#] `[:json_extract_scalar ~(keywordize from#) ~path#])
                (~'approx-distinct [a#] `[:approx_distinct ~(keywordize a#)])
                (~'rand [] [:rand])
                (~'count [] [:count :*])
                (~'count-if [a#] `[:count_if ~(keywordize a#)])
                (~'sum [a#] `[:sum ~(keywordize a#)])
                (~'avg [a#] `[:avg ~(keywordize a#)])
                (~'coalesce [& args#] `[:coalesce ~@(map keywordize args#)])
                (~'date [arg#] `[:date ~(keywordize arg#)])
                (~'+ [a# b#] `[:+ ~(keywordize a#) ~(keywordize b#)])
                (~'- [a# b#] `[:- ~(keywordize a#) ~(keywordize b#)])
                (~'* [a# b#] `[:* ~(keywordize a#) ~(keywordize b#)])
                (~'/ [a# b#] `[:/ ~(keywordize a#) ~(keywordize b#)])]
               ~expr))

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

;;; allowing referencing just declared vars by substituting. if we
;;; don't want duplication, can use multiple mutates but that will
;;; cause nesting
(defmacro mutate [ds & forms]
  (let [pairs (partition 2 forms)
        new-cols (m/mexpand-all `(m/symbol-macrolet [~@forms]
                                                    ~(mapv named-expr pairs)))]
    `(precedence-merge ~ds {:select (concat (get-selection-cols ~ds)
                                            (expand-expr ~new-cols))})))

(defmacro mutate-grouped [ds groups & forms]
  (let [pairs (partition 2 forms)
        new-cols (m/mexpand-all `(m/symbol-macrolet [~@forms]
                                                    [[[:over ~@(mapv (partial named-expr
                                                                              {:partition-by (mapv keywordize groups)})
                                                                     pairs)]]]))]
    `(precedence-merge ~ds {:select (concat (get-selection-cols ~ds)
                                            (expand-expr ~new-cols))})))

(defn limit [ds limit]
  (precedence-merge ds {:limit limit}))

;;; TODO: naming columns the same as functions causes an infinite loop e.g. sum (sum foo)
;;; TODO: would be cool to auto-name, probably need to change the form for this
(defmacro summarize [ds & forms]
  (let [pairs (partition 2 forms)
        replace-form `{:select (m/symbol-macrolet [~@forms]
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

;;; TODO: allow creating cols to group on?
(defmacro group [ds groups & forms]
  `(-> ~ds
       ~@(map (partial rewrite-for-group groups) forms)
       ~(if (contains-summarize forms)
          `(update-in [:select] (fn [a# b#] (concat b# a#)) ~(mapv keywordize groups))
          `(identity))
       ~(if (contains-summarize forms)
          `(merge {:group-by ~(mapv keywordize groups)})
          `(identity))))

(defmacro count-by [ds & groups]
  `(-> ~ds
       (group [~@groups]
              (~'summarize ~'n (~'count)))
       (order-by (~'desc ~'n))))

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
                   (mapv (partial rename-col [] :q1) q1-cols)
                   (->> q2-cols
                        (remove (set (if (empty? join-cols) q1-cols join-cols))) ; used to maintain order
                        (mapv (partial rename-col q1-cols :q2)))))
     :from [[query1 :q1]]
     join-type-kw [[query2 :q2]
                   (->> (if-not (empty? join-cols)
                          join-cols
                          (sets/intersection (set q1-cols) (set q2-cols)))
                        (mapv (fn [col] [:= (scope-col :q1 col) (scope-col :q2 col)]))
                        (into [:and]))]}))

;;; TODO: extract common macro
(defmacro inner-join [query1 query2 & {:keys [using suffix]}]
  (let [q1 (m/mexpand-all query1)
        q2 (m/mexpand-all query2)]
    `(join :inner-join ~q1 ~q2 ~(mapv keywordize using) ~(or suffix ""))))

(defmacro left-join [query1 query2 & {:keys [using suffix]}]
  (let [q1 (m/mexpand-all query1)
        q2 (m/mexpand-all query2)]
    `(join :left-join ~q1 ~q2 ~(mapv keywordize using) ~(or suffix ""))))


(defn -main [& args]
  (binding [*ns* (find-ns 'sqlgen.core)]
    (-> (read)
        eval
        (sql/format :inline true)
        (get 0)
        println)))


;;; diff between summarise and select and mutate: select is about
;;; preds to match existing stuff. it replaces cols, allows renaming
;;; with :as, but does not allow creating cols from new expressions.
;;; summarise replaces all cols, it has a different form to make it
;;; convenient to create a new set of columns from expressions. most
;;; useful for summarising. mutate adds to the existing selection with
;;; a similar form to summarize. summarize and mutate allow referring
;;; to just created columns. they will resubstitute expressions to
;;; make this possible. using another summarize or mutate avoids this
;;; at the cost of nesting.


;;; how is this different from dbplyr? don't need a dbi connection.
;;; but mostly it breaks with trying to have exactly the same
;;; semantics as dplyr. it is just a bit closer to sql which makes the
;;; generated output more predictable and understandable.
