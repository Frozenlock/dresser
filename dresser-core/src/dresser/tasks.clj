(ns dresser.tasks)


;; How to know to which 'layer' to apply the task?
;; We can at least have use the dresser-id, but do we need higher resolution?



;; If we add cljs support https://clojureverse.org/t/how-do-i-get-env-data-in-cljs-macros/4105

(defn- save-env [locals form]
  (let [quoted-form `(quote ~form)]
    (if locals
      `(list `let [~@(for [local locals,
                           let-arg [`(quote ~local)
                                    `(list `quote ~local)]]
                       let-arg)]
             ~quoted-form)
      quoted-form)))

(defmacro mymacro
  [& sigs]
  (save-env (keys &env) &form))

(def abc "hello")

(def aaa (atom :--1))

(fn [p]
  (let [ddd 1234
        zzz "allo"
        bbb @aaa]
    (mymacro
     (fn [a]
       (prn abc)
       (prn zzz)
       (+ 2 3 a)))))
