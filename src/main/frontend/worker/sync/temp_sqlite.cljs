(ns frontend.worker.sync.temp-sqlite
  "Temporary sqlite helpers for db sync uploads."
  (:require [cljs-bean.core :as bean]
            [datascript.core :as d]
            [datascript.storage :refer [IStorage]]
            [logseq.db.common.sqlite :as common-sqlite]
            [logseq.db.sqlite.util :as sqlite-util]
            [lambdaisland.glogi :as log]
            [promesa.core :as p]))

(defn- upsert-addr-content!
  [^js db data]
  (.transaction
   db
   (fn [tx]
     (doseq [item data]
       (.exec tx #js {:sql (str "INSERT INTO kvs (addr, content, addresses) "
                                "values ($addr, $content, $addresses) "
                                "on conflict(addr) do update set content = $content, addresses = $addresses")
                      :bind item})))))

(defn- restore-data-from-addr
  [^js db addr]
  (when-let [result (-> (.exec db #js {:sql "select content, addresses from kvs where addr = ?"
                                       :bind #js [addr]
                                       :rowMode "array"})
                        first)]
    (let [[content addresses] (bean/->clj result)
          addresses (when addresses (js/JSON.parse addresses))
          data (sqlite-util/transit-read content)]
      (if (and addresses (map? data))
        (assoc data :addresses addresses)
        data))))

(defn new-temp-sqlite-storage
  [^js db]
  (reify IStorage
    (-store [_ addr+data-seq _delete-addrs]
      (let [data (map
                  (fn [[addr data]]
                    (let [data' (if (map? data) (dissoc data :addresses) data)
                          addresses (when (map? data)
                                      (when-let [addresses (:addresses data)]
                                        (js/JSON.stringify (bean/->js addresses))))]
                      #js {:$addr addr
                           :$content (sqlite-util/transit-write data')
                           :$addresses addresses}))
                  addr+data-seq)]
        (upsert-addr-content! db data)))
    (-restore [_ addr]
      (restore-data-from-addr db addr))))

(defn <get-upload-temp-sqlite-pool
  [{:keys [*pool sqlite pool-name fail-fast-f]}]
  (if-let [pool @*pool]
    (p/resolved pool)
    (if-let [sqlite* sqlite]
      (p/let [^js pool (.installOpfsSAHPoolVfs ^js sqlite* #js {:name pool-name
                                                                :initialCapacity 20})]
        (reset! *pool pool)
        pool)
      (fail-fast-f :db-sync/missing-field {:field :sqlite}))))

(defn upload-temp-sqlite-path
  []
  (str "/upload-" (random-uuid) ".sqlite"))

(defn <create-temp-sqlite-db!
  [{:keys [get-pool-f upload-path-f]}]
  (p/let [^js pool (get-pool-f)
          capacity (.getCapacity pool)
          _ (when (zero? capacity)
              (.unpauseVfs pool))
          path (upload-path-f)
          _ (log/info :db-sync/upload-debug
                      {:stage :temp-sqlite/create-db/start
                       :path path
                       :capacity capacity})
          ^js db (new (.-OpfsSAHPoolDb pool) path)]
    (common-sqlite/create-kvs-table! db)
    (log/info :db-sync/upload-debug
              {:stage :temp-sqlite/create-db/done
               :path path
               :capacity capacity})
    {:db db
     :path path
     :pool pool}))

(defn <create-temp-sqlite-conn
  [schema datoms create-db-f]
  (log/info :db-sync/upload-debug
            {:stage :temp-sqlite/create-conn/start})
  (p/let [{:keys [db path pool]} (create-db-f)
          storage (new-temp-sqlite-storage db)
          _ (log/info :db-sync/upload-debug
                      {:stage :temp-sqlite/create-conn/before-conn-from-datoms
                       :path path})
          conn (d/conn-from-datoms datoms schema {:storage storage})]
    (log/info :db-sync/upload-debug
              {:stage :temp-sqlite/create-conn/done
               :path path})
    {:db db
     :conn conn
     :path path
     :pool pool}))

(defn <remove-upload-temp-sqlite-db-file!
  [pool-name path]
  (-> (p/let [^js root (.getDirectory js/navigator.storage)
              ^js dir (.getDirectoryHandle root (str "." pool-name))]
        (.removeEntry dir (subs path 1)))
      (p/catch
       (fn [error]
         (if (= "NotFoundError" (.-name error))
           nil
           (p/rejected error))))))

(defn cleanup-temp-sqlite!
  [{:keys [db conn path]} remove-file-f]
  (when conn
    (reset! conn nil))
  (when db
    (.close db))
  (when path
    (remove-file-f path)))
