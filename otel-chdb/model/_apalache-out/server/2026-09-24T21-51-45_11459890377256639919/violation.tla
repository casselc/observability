---------------------------- MODULE counterexample ----------------------------

EXTENDS lifetimeEqQmax

(* Constant initialization state *)
ConstInit == TRUE

(* Initial state [_transition(0)] *)
State0 ==
  lifetimeEqQmax_partLifetime_acked = FALSE
    /\ lifetimeEqQmax_partLifetime_badRead = FALSE
    /\ lifetimeEqQmax_partLifetime_events = {}
    /\ lifetimeEqQmax_partLifetime_gcDone = FALSE
    /\ lifetimeEqQmax_partLifetime_now = 0
    /\ lifetimeEqQmax_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >> })
    /\ lifetimeEqQmax_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> FALSE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> FALSE,
          view |-> {}]
      >>})
    /\ lifetimeEqQmax_partLifetime_writerUp = TRUE

(* State1 [_transition(0)] *)
State1 ==
  lifetimeEqQmax_partLifetime_acked = FALSE
    /\ lifetimeEqQmax_partLifetime_badRead = FALSE
    /\ lifetimeEqQmax_partLifetime_events = {"insert"}
    /\ lifetimeEqQmax_partLifetime_gcDone = FALSE
    /\ lifetimeEqQmax_partLifetime_now = 0
    /\ lifetimeEqQmax_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >> })
    /\ lifetimeEqQmax_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> FALSE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> FALSE,
          view |-> {}]
      >>})
    /\ lifetimeEqQmax_partLifetime_writerUp = TRUE

(* State2 [_transition(4)] *)
State2 ==
  lifetimeEqQmax_partLifetime_acked = FALSE
    /\ lifetimeEqQmax_partLifetime_badRead = FALSE
    /\ lifetimeEqQmax_partLifetime_events = {"insert"}
    /\ lifetimeEqQmax_partLifetime_gcDone = FALSE
    /\ lifetimeEqQmax_partLifetime_now = 0
    /\ lifetimeEqQmax_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >> })
    /\ lifetimeEqQmax_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> TRUE,
          view |-> {1}]
      >>})
    /\ lifetimeEqQmax_partLifetime_writerUp = TRUE

(* State3 [_transition(0)] *)
State3 ==
  lifetimeEqQmax_partLifetime_acked = FALSE
    /\ lifetimeEqQmax_partLifetime_badRead = FALSE
    /\ lifetimeEqQmax_partLifetime_events = {"insert"}
    /\ lifetimeEqQmax_partLifetime_gcDone = FALSE
    /\ lifetimeEqQmax_partLifetime_now = 0
    /\ lifetimeEqQmax_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> {},
            since |-> 0,
            st |-> Variant("Absent", [tag |-> "UNIT"])]
        >> })
    /\ lifetimeEqQmax_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> TRUE,
          view |-> {1}]
      >>})
    /\ lifetimeEqQmax_partLifetime_writerUp = TRUE

(* State4 [_transition(1)] *)
State4 ==
  lifetimeEqQmax_partLifetime_acked = FALSE
    /\ lifetimeEqQmax_partLifetime_badRead = FALSE
    /\ lifetimeEqQmax_partLifetime_events = { "insert", "merge" }
    /\ lifetimeEqQmax_partLifetime_gcDone = FALSE
    /\ lifetimeEqQmax_partLifetime_now = 0
    /\ lifetimeEqQmax_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> { 1, 2 },
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >> })
    /\ lifetimeEqQmax_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> TRUE,
          view |-> {1}]
      >>})
    /\ lifetimeEqQmax_partLifetime_writerUp = TRUE

(* State5 [_transition(13)] *)
State5 ==
  lifetimeEqQmax_partLifetime_acked = FALSE
    /\ lifetimeEqQmax_partLifetime_badRead = FALSE
    /\ lifetimeEqQmax_partLifetime_events = { "insert", "merge" }
    /\ lifetimeEqQmax_partLifetime_gcDone = FALSE
    /\ lifetimeEqQmax_partLifetime_now = 1
    /\ lifetimeEqQmax_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> { 1, 2 },
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >> })
    /\ lifetimeEqQmax_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 0,
          refreshing |-> TRUE,
          view |-> {1}]
      >>})
    /\ lifetimeEqQmax_partLifetime_writerUp = TRUE

(* State6 [_transition(7)] *)
State6 ==
  lifetimeEqQmax_partLifetime_acked = FALSE
    /\ lifetimeEqQmax_partLifetime_badRead = FALSE
    /\ lifetimeEqQmax_partLifetime_events = { "insert", "merge" }
    /\ lifetimeEqQmax_partLifetime_gcDone = FALSE
    /\ lifetimeEqQmax_partLifetime_now = 1
    /\ lifetimeEqQmax_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> { 1, 2 },
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >> })
    /\ lifetimeEqQmax_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {1}, read |-> {}, running |-> TRUE, started |-> 1],
          refreshedAt |-> 0,
          refreshing |-> TRUE,
          view |-> {1}]
      >>})
    /\ lifetimeEqQmax_partLifetime_writerUp = TRUE

(* State7 [_transition(5)] *)
State7 ==
  lifetimeEqQmax_partLifetime_acked = FALSE
    /\ lifetimeEqQmax_partLifetime_badRead = FALSE
    /\ lifetimeEqQmax_partLifetime_events = { "insert", "merge" }
    /\ lifetimeEqQmax_partLifetime_gcDone = FALSE
    /\ lifetimeEqQmax_partLifetime_now = 1
    /\ lifetimeEqQmax_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> { 1, 2 },
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >> })
    /\ lifetimeEqQmax_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {1}, read |-> {}, running |-> TRUE, started |-> 1],
          refreshedAt |-> 1,
          refreshing |-> TRUE,
          view |-> {3}]
      >>})
    /\ lifetimeEqQmax_partLifetime_writerUp = TRUE

(* State8 [_transition(13)] *)
State8 ==
  lifetimeEqQmax_partLifetime_acked = FALSE
    /\ lifetimeEqQmax_partLifetime_badRead = FALSE
    /\ lifetimeEqQmax_partLifetime_events = { "insert", "merge" }
    /\ lifetimeEqQmax_partLifetime_gcDone = FALSE
    /\ lifetimeEqQmax_partLifetime_now = 2
    /\ lifetimeEqQmax_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> { 1, 2 },
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >> })
    /\ lifetimeEqQmax_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {1}, read |-> {}, running |-> TRUE, started |-> 1],
          refreshedAt |-> 1,
          refreshing |-> TRUE,
          view |-> {3}]
      >>})
    /\ lifetimeEqQmax_partLifetime_writerUp = TRUE

(* State9 [_transition(2)] *)
State9 ==
  lifetimeEqQmax_partLifetime_acked = FALSE
    /\ lifetimeEqQmax_partLifetime_badRead = FALSE
    /\ lifetimeEqQmax_partLifetime_events = { "cleanup", "insert", "merge" }
    /\ lifetimeEqQmax_partLifetime_gcDone = FALSE
    /\ lifetimeEqQmax_partLifetime_now = 2
    /\ lifetimeEqQmax_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Deleted", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> { 1, 2 },
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >> })
    /\ lifetimeEqQmax_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {1}, read |-> {}, running |-> TRUE, started |-> 1],
          refreshedAt |-> 1,
          refreshing |-> TRUE,
          view |-> {3}]
      >>})
    /\ lifetimeEqQmax_partLifetime_writerUp = TRUE

(* State10 [_transition(8)] *)
State10 ==
  lifetimeEqQmax_partLifetime_acked = FALSE
    /\ lifetimeEqQmax_partLifetime_badRead = TRUE
    /\ lifetimeEqQmax_partLifetime_events
      = { "badRead", "cleanup", "insert", "merge" }
    /\ lifetimeEqQmax_partLifetime_gcDone = FALSE
    /\ lifetimeEqQmax_partLifetime_now = 2
    /\ lifetimeEqQmax_partLifetime_parts
      = SetAsFun({ <<
          1, [covers |-> {},
            since |-> 0,
            st |-> Variant("Deleted", [tag |-> "UNIT"])]
        >>,
        <<
          2, [covers |-> {},
            since |-> 0,
            st |-> Variant("Outdated", [tag |-> "UNIT"])]
        >>,
        <<
          3, [covers |-> { 1, 2 },
            since |-> 0,
            st |-> Variant("Active", [tag |-> "UNIT"])]
        >> })
    /\ lifetimeEqQmax_partLifetime_readers
      = SetAsFun({<<
        1, [attached |-> TRUE,
          q |-> [parts |-> {}, read |-> {}, running |-> FALSE, started |-> 0],
          refreshedAt |-> 1,
          refreshing |-> TRUE,
          view |-> {3}]
      >>})
    /\ lifetimeEqQmax_partLifetime_writerUp = TRUE

(* The following formula holds true in the last state and violates the invariant *)
InvariantViolation == lifetimeEqQmax_partLifetime_badRead

================================================================================
(* Created by Apalache on Thu Sep 24 21:53:02 UTC 2026 *)
(* https://github.com/apalache-mc/apalache *)
