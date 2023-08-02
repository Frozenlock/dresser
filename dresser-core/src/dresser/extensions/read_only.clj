(ns dresser.extensions.read-only
  (:require [dresser.extension :as ext]
            [dresser.protocols :as dp]))

(defn read-only-exception
  [method-sym]
  (ex-info "This dresser is read-only"
           {:method (name method-sym)}))

(ext/defext read-only
  "Disables all methods that could write to the dresser."
  []
  {:wrap-configs (into
                  {}
                  (for [[sym m] dp/dresser-methods
                        :when (:w m)]
                    [sym {:wrap (fn [_method]
                                  ;; Don't use the method and
                                  ;; throw an exception instead
                                  (fn [& args]
                                    (throw (read-only-exception sym))))}]))})
