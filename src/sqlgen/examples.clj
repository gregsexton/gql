(ns sqlgen.examples
  (:require [sqlgen.core :refer :all]
            [honey.sql :as sql]))

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
    (mutate not-is-closed (not is-closed))
    (order-by is-closed
              (desc status)
              (rand)
              (desc (json-extract-scalar data "$.topic")))
    (limit 100)
    (sql/format :inline true))

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
    (sql/format :inline true))

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
    (count-by topic)
    (sql/format :inline true))

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
    (left-join (-> (table src_wa_fastdesk_tickets)
                   (group [topic] (summarize n (count)))))   ;this is inlining
    (sql/format :inline true))

(-> (table src_wa_fastdesk_tickets)
    (where (> ds "<DATEID-2>"))
    (select topic data)
    (inner-join (table foo))           ;this is how you join with an existing table
    (sql/format :inline true))

(-> (table src-wa-fastdesk-tickets)
    (select id data)
    (left-join (-> (table src-wa-fastdesk-tickets)
                   (where (> id 5))
                   (select id data))
               :using [id]
               :suffix "_filtered")
    (sql/format :inline true))

(let [users (-> (table wa-support-funnel)
                (where (>= ds "<DATEID-14>"))
                (count-by user-id)
                (order-by (rand))
                (limit 100))]
  (-> users
      (left-join (-> (table wa-support-funnel)
                     (select user-id event-time event-type whatsapp-analytic-id whatsapp-faq-search-unique-id ticket-id))
                 :using [user-id])
      (sql/format :inline true)
      (get 0)
      println))

(let [users (-> (table wa-support-funnel)
                (where (>= ds "<DATEID-14>"))
                (count-by user-id)
                (order-by (rand))
                (limit 100))
      tickets (-> (table src-wa-fastdesk-tickets)
                  (where (= ds "<DATEID>"))
                  (mutate topic (json-extract-scalar data "$.topic"))
                  (select (id :as ticket-id) topic))]
  (-> users
      (left-join (-> (table wa-support-funnel)
                     (select user-id event-time event-type whatsapp-analytic-id whatsapp-faq-search-unique-id ticket-id))
                 :using [user-id])
      (left-join tickets)
      (sql/format :inline true)))

(let [users (-> (table wa-support-funnel)
                (where (>= ds "<DATEID-14>"))
                (count-by user-id)
                (order-by (rand))
                (limit 100))
      tickets (-> (table src-wa-fastdesk-tickets)
                  (where (= ds "<DATEID>"))
                  (mutate topic (json-extract-scalar data "$.topic"))
                  (select (id :as ticket-id) topic))
      site-events (-> (table wa-support-funnel-site-events)
                      (where (>= ds "<DATEID-14>")
                             (= event-type "load")
                             (not= primary_cmsid nil))
                      (group [whatsapp-analytic-id]
                             (summarize n-loads (count))))]
  (-> users
      (left-join (-> (table wa-support-funnel)
                     (select user-id event-time event-type whatsapp-analytic-id whatsapp-faq-search-unique-id ticket-id))
                 :using [user-id])
      (left-join tickets :using [ticket-id])
      (left-join site-events :using [whatsapp-analytic-id])
      (sql/format :inline true :pretty true)
      (get 0)
      println))


(let [users (-> (table wa-support-funnel)
                (where (>= ds "<DATEID-14>"))
                (count-by user-id)
                (order-by (rand))
                (limit 100))
      tickets (-> (table src-wa-fastdesk-tickets)
                  (where (= ds "<DATEID>"))
                  (mutate topic (json-extract-scalar data "$.topic"))
                  (select (id :as ticket-id) topic))
      site-events (-> (table wa-support-funnel-site-events)
                      (where (>= ds "<DATEID-14>")
                             (= event-type "load"))
                      (group [whatsapp-analytic-id]
                             (summarize n-loads (count)
                                        n-real-loads (count-if (not= primary_cmsid nil)))))]
  (-> users
      (left-join (-> (table wa-support-funnel)
                     (select user-id event-time event-type whatsapp-analytic-id whatsapp-faq-search-unique-id ticket-id))
                 :using [user-id])
      (left-join tickets :using [ticket-id])
      (left-join site-events :using [whatsapp-analytic-id])))

(-> (table foo)
    (select a b)
    (inner-join (-> (table bar)
                    (select a c)
                    (mutate foo (coalesce e f g))))
    (sql/format :inline true))

(-> (table foo)
    (group [id]
           (mutate n (count)))
    (sql/format :inline true))

(-> (table foo)
    (group [] (mutate n (count)))       ;TODO: broken
    (sql/format :inline true))

(-> (table foo)
    (group [foo] (summarize n (count)))
    (sql/format :inline true))

(-> (table foo)
    (count-by foo)
    (sql/format :inline true))

(-> (table foo)
    (mutate n (if-else flag 1 0))
    (sql/format :inline true))

(let [tickets (-> (table src_wa_fastdesk_tickets)
                  (where (= ds "<DATEID>"))
                  (mutate topic (json-extract-scalar data "$.topic"))
                  (select (id :as ticket-id) topic))]
  (-> (table wa_support_funnel)
      (where (= ds "<DATEID-1>")
             (= event_type "ticket_creation"))
      (select event_type ticket-id)
      (left-join tickets)
      (count-by topic)
      (sql/format :inline true)))

(-> (table foo)
    (where (in col 1 2 3))
    (sql/format :inline true))

(-> (table foo)
    (group [a b] (summarize n (count)))
    (mutate x (= n 3))
    (sql/format :inline true))

(-> (table foo)
    (mutate x (case-when (= f 3) "foo"
                         (> f 2) "bar"
                         else "medium"))
    (sql/format :inline true))

(-> (table foo)
    (order-by x (rand) (desc y) (desc (rand)))
    (mutate x (rand))
    (sql/format :inline true))

(-> (table foo)
    (mutate x (cast y VARCHAR))
    (mutate keys (-> json-payload
                     (json-extract "$.body")
                     (cast "MAP<VARCHAR, JSON>")
                     (map-keys)))
    (slice-head 5)
    (sql/format :inline true))

(-> (table foo)
    (slice-sample 5)
    (sql/format :inline true))

;;; this was added to check propagating * in selects and not ending up with x twice
(-> (table foo)
    (mutate x 5)
    (mutate y 7)
    (sql/format :inline true))

(-> (table foo)
    (group [x]
           (mutate n-sum (sum n)))
    (group []
           (mutate n-sum (sum n)))
    (sql/format :inline true))

;;; TODO:
(-> (table foo)
    (group []
           (order-by z)
           (mutate n-sum (sum n)))
    (sql/format :inline true))

;;; TODO:
(-> (table foo)
    (mutate n (count-if (and (> y 7) (< x 2))))
    (sql/format :inline true))
